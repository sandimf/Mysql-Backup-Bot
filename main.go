package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// Env helpers
func getenv(key, def string) string { v := os.Getenv(key); if v == "" { return def }; return v }

// Config
var (
	mysqlHost = getenv("MYSQL_HOST", "127.0.0.1")
	mysqlPort = getenv("MYSQL_PORT", "3306")
	mysqlUser = getenv("MYSQL_USER", "root")
	mysqlPass = os.Getenv("MYSQL_PASS", "datakgs25") // kosong = tanpa password
	mysqlDB   = os.Getenv("MYSQL_DB", "klinik_apps")    // wajib

	backupDir      = getenv("BACKUP_DIR", "/var/backups/mysql")
	retentionDays  = getenv("RETENTION_DAYS", "7")
	cronExpr       = os.Getenv("CRON_EXPR") // contoh: "0 2 * * *" (tiap jam 02:00)

	botToken = os.Getenv("TELEGRAM_BOT_TOKEN") // wajib
	chatID   = os.Getenv("TELEGRAM_CHAT_ID")   // wajib (grup)

	runOnce = os.Getenv("RUN_ONCE") // jika "1": lakukan 1x backup lalu exit (untuk cron OS)
)

// Telegram API
const telegramAPI = "https://api.telegram.org/bot%s/%s"

func main() {
	if mysqlDB == "" || botToken == "" || chatID == "" {
		fmt.Println("[ERR] MYSQL_DB, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID wajib di-set")
		os.Exit(1)
	}

	// Buat folder backup bila belum ada
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		fmt.Println("[ERR] create backup dir:", err)
		os.Exit(1)
	}

	// Mode runOnce untuk dipakai dengan cron/systemd
	if runOnce == "1" {
		if err := doBackupAndSend(context.Background()); err != nil {
			fmt.Println("[ERR] backup:", err)
			os.Exit(1)
		}
		if err := applyRetention(); err != nil { fmt.Println("[WARN] retention:", err) }
		return
	}

	// Jika pakai CRON internal
	if cronExpr != "" {
		c := cron.New()
		_, err := c.AddFunc(cronExpr, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
			defer cancel()
			if err := doBackupAndSend(ctx); err != nil {
				fmt.Println("[ERR] scheduled backup:", err)
			}
			if err := applyRetention(); err != nil { fmt.Println("[WARN] retention:", err) }
		})
		if err != nil { fmt.Println("[ERR] cron expr:", err); os.Exit(1) }
		c.Start()
		fmt.Println("[OK] Scheduler aktif dengan CRON_EXPR:", cronExpr)
	}

	// Polling Telegram untuk perintah /backup dan /chatid
	fmt.Println("[OK] Bot polling Telegram…")
	pollTelegram()
}

func pollTelegram() {
	var offset int
	client := &http.Client{ Timeout: 30 * time.Second }
	for {
		url := fmt.Sprintf(telegramAPI, botToken, "getUpdates")
		body := fmt.Sprintf("offset=%d&timeout=25", offset)
		req, _ := http.NewRequest("POST", url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := client.Do(req)
		if err != nil { time.Sleep(3 * time.Second); continue }
		var data struct { Ok bool `json:"ok"`; Result []struct { UpdateID int `json:"update_id"`; Message *struct {
			MessageID int `json:"message_id"`
			Chat struct { ID int64 `json:"id"`; Type string `json:"type"` } `json:"chat"`
			Text string `json:"text"`
			From *struct { ID int64 `json:"id"`; Username string `json:"username"` } `json:"from"`
		} `json:"message"` } `json:"result"` }
		json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()
		for _, u := range data.Result {
			offset = u.UpdateID + 1
			if u.Message == nil { continue }
			text := strings.TrimSpace(u.Message.Text)
			switch {
			case strings.HasPrefix(text, "/backup"):
				// Selalu backup dan kirim ke grup utama (chatID dari env)
				go func() {
					sendText(u.Message.Chat.ID, "Memulai backup… mohon tunggu.")
					if err := doBackupAndSend(context.Background()); err != nil {
						sendText(u.Message.Chat.ID, fmt.Sprintf("Backup gagal: %v", err))
						return
					}
					sendText(u.Message.Chat.ID, "Backup selesai dan dikirim ke grup.")
				}()
			case strings.HasPrefix(text, "/chatid"):
				sendText(u.Message.Chat.ID, fmt.Sprintf("Chat ID: %d", u.Message.Chat.ID))
			}
		}
	}
}

func sendText(chat int64, text string) {
	client := &http.Client{ Timeout: 15 * time.Second }
	url := fmt.Sprintf(telegramAPI, botToken, "sendMessage")
	payload := strings.NewReader(fmt.Sprintf("chat_id=%d&text=%s", chat, urlEncode(text)))
	req, _ := http.NewRequest("POST", url, payload)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client.Do(req)
}

func urlEncode(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "&", "%26"), "+", "%2B") }

func doBackupAndSend(ctx context.Context) error {
	// Nama file
	stamp := time.Now().Format("20060102_150405")
	fname := fmt.Sprintf("%s_%s.sql.gz", mysqlDB, stamp)
	fpath := filepath.Join(backupDir, fname)

	// Jalankan mysqldump -> gzip (pakai env MYSQL_PWD agar password aman)
	dumpCmd := fmt.Sprintf("mysqldump -h %s -P %s -u %s --single-transaction --quick --routines --triggers --events --set-gtid-purged=OFF %s | gzip -c > %s",
		shEscape(mysqlHost), shEscape(mysqlPort), shEscape(mysqlUser), shEscape(mysqlDB), shEscape(fpath))

	cmd := exec.CommandContext(ctx, "bash", "-lc", dumpCmd)
	if mysqlPass != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+mysqlPass)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mysqldump error: %v, out: %s", err, string(out))
	}

	// Kirim ke Telegram sebagai dokumen (selalu ke grup utama dari env)
	if err := sendDocument(fpath, fname); err != nil {
		return err
	}
	return nil
}

func shEscape(s string) string {
	// sederhana untuk argumen
	return strings.ReplaceAll(s, " ", "\\ ")
}

func sendDocument(path, displayName string) error {
	file, err := os.Open(path)
	if err != nil { return err }
	defer file.Close()

	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	_ = w.WriteField("chat_id", chatID)
	_ = w.WriteField("disable_content_type_detection", "true")
	_ = w.WriteField("caption", fmt.Sprintf("MySQL backup %s", time.Now().Format(time.RFC3339)))

	fw, err := w.CreateFormFile("document", displayName)
	if err != nil { return err }
	if _, err := io.Copy(fw, file); err != nil { return err }
	w.Close()

	client := &http.Client{ Timeout: 5 * time.Minute }
	url := fmt.Sprintf(telegramAPI, botToken, "sendDocument")
	req, _ := http.NewRequest("POST", url, &b)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := client.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram sendDocument failed: %s", string(body))
	}
	return nil
}

func applyRetention() error {
	days, _ := strconv.Atoi(getenv("RETENTION_DAYS", "7"))
	if days <= 0 { return nil }
	cut := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	entries, err := os.ReadDir(backupDir)
	if err != nil { return err }
	for _, e := range entries {
		if e.IsDir() { continue }
		p := filepath.Join(backupDir, e.Name())
		info, err := os.Stat(p)
		if err != nil { continue }
		if info.ModTime().Before(cut) && strings.HasSuffix(e.Name(), ".sql.gz") {
			_ = os.Remove(p)
		}
	}
	return nil
}

// util: random string (tidak dipakai saat ini, disimpan bila perlu)
func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	for i := range b { b[i] = "abcdefghijklmnopqrstuvwxyz0123456789"[int(b[i])%36] }
	return string(b)
}