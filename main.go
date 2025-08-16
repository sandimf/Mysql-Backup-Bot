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
func getenv(key, def string) string { 
	v := os.Getenv(key)
	if v == "" { 
		return def 
	}
	return v 
}

// Config
var (
	mysqlHost = getenv("MYSQL_HOST", "127.0.0.1")
	mysqlPort = getenv("MYSQL_PORT", "3306")
	mysqlUser = getenv("MYSQL_USER", "root")
	mysqlPass = getenv("MYSQL_PASS", "") // kosong = tanpa password
	mysqlDB   = getenv("MYSQL_DB", "")   // wajib
	
	// Tabel yang akan di-backup (spesifik untuk klinik_apps)
	backupTables  = getenv("BACKUP_TABLES", "klinik_apps")
	
	backupDir     = getenv("BACKUP_DIR", "/var/backups/mysql")
	retentionDays = getenv("RETENTION_DAYS", "7")
	cronExpr      = os.Getenv("CRON_EXPR") // contoh: "0 2 * * *" (tiap jam 02:00)

	botToken = getenv("TELEGRAM_BOT_TOKEN", "") // wajib
	chatID   = getenv("TELEGRAM_CHAT_ID", "")   // wajib (grup)

	runOnce = os.Getenv("RUN_ONCE") // jika "1": lakukan 1x backup lalu exit (untuk cron OS)
)

// Telegram API
const telegramAPI = "https://api.telegram.org/bot%s/%s"

func main() {
	// Validasi environment variables wajib
	if mysqlDB == "" {
		fmt.Println("[ERR] MYSQL_DB wajib di-set")
		os.Exit(1)
	}
	if botToken == "" {
		fmt.Println("[ERR] TELEGRAM_BOT_TOKEN wajib di-set")
		os.Exit(1)
	}
	if chatID == "" {
		fmt.Println("[ERR] TELEGRAM_CHAT_ID wajib di-set")
		os.Exit(1)
	}

	// Buat folder backup bila belum ada
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		fmt.Println("[ERR] Gagal membuat direktori backup:", err)
		os.Exit(1)
	}

	fmt.Printf("[INFO] Backup akan dilakukan untuk tabel: %s dari database: %s\n", backupTables, mysqlDB)

	// Mode runOnce untuk dipakai dengan cron/systemd
	if runOnce == "1" {
		fmt.Println("[INFO] Mode run-once aktif, melakukan backup sekali...")
		if err := doBackupAndSend(context.Background()); err != nil {
			fmt.Printf("[ERR] Backup gagal: %v\n", err)
			os.Exit(1)
		}
		if err := applyRetention(); err != nil { 
			fmt.Printf("[WARN] Retention error: %v\n", err) 
		}
		fmt.Println("[OK] Backup selesai")
		return
	}

	// Jika pakai CRON internal
	if cronExpr != "" {
		c := cron.New()
		_, err := c.AddFunc(cronExpr, func() {
			fmt.Printf("[INFO] Menjalankan backup terjadwal pada %s\n", time.Now().Format("2006-01-02 15:04:05"))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
			defer cancel()
			
			if err := doBackupAndSend(ctx); err != nil {
				fmt.Printf("[ERR] Scheduled backup gagal: %v\n", err)
				// Kirim notifikasi error ke Telegram
				sendText(parseChatID(chatID), fmt.Sprintf("❌ Backup terjadwal gagal: %v", err))
			} else {
				fmt.Println("[OK] Scheduled backup berhasil")
			}
			
			if err := applyRetention(); err != nil { 
				fmt.Printf("[WARN] Retention error: %v\n", err) 
			}
		})
		if err != nil { 
			fmt.Printf("[ERR] Invalid CRON expression: %v\n", err)
			os.Exit(1) 
		}
		c.Start()
		fmt.Printf("[OK] Scheduler aktif dengan CRON_EXPR: %s\n", cronExpr)
	}

	// Polling Telegram untuk perintah /backup dan /chatid
	fmt.Println("[OK] Bot polling Telegram untuk menerima perintah...")
	pollTelegram()
}

func parseChatID(chatIDStr string) int64 {
	chatIDInt, _ := strconv.ParseInt(chatIDStr, 10, 64)
	return chatIDInt
}

func pollTelegram() {
	var offset int
	client := &http.Client{ Timeout: 30 * time.Second }
	
	for {
		url := fmt.Sprintf(telegramAPI, botToken, "getUpdates")
		body := fmt.Sprintf("offset=%d&timeout=25", offset)
		
		req, err := http.NewRequest("POST", url, strings.NewReader(body))
		if err != nil {
			fmt.Printf("[WARN] Error creating request: %v\n", err)
			time.Sleep(3 * time.Second)
			continue
		}
		
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := client.Do(req)
		if err != nil { 
			fmt.Printf("[WARN] Polling error: %v\n", err)
			time.Sleep(3 * time.Second)
			continue 
		}
		
		var data struct { 
			Ok bool `json:"ok"`
			Result []struct { 
				UpdateID int `json:"update_id"`
				Message *struct {
					MessageID int `json:"message_id"`
					Chat struct { 
						ID   int64  `json:"id"`
						Type string `json:"type"` 
					} `json:"chat"`
					Text string `json:"text"`
					From *struct { 
						ID       int64  `json:"id"`
						Username string `json:"username"` 
					} `json:"from"`
				} `json:"message"` 
			} `json:"result"` 
		}
		
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			fmt.Printf("[WARN] JSON decode error: %v\n", err)
		}
		resp.Body.Close()
		
		for _, u := range data.Result {
			offset = u.UpdateID + 1
			if u.Message == nil { continue }
			
			text := strings.TrimSpace(u.Message.Text)
			userInfo := ""
			if u.Message.From != nil {
				userInfo = fmt.Sprintf(" (dari @%s)", u.Message.From.Username)
			}
			
			switch {
			case strings.HasPrefix(text, "/backup"):
				fmt.Printf("[INFO] Perintah backup diterima%s\n", userInfo)
				go func() {
					sendText(u.Message.Chat.ID, "🔄 Memulai backup tabel klinik_apps... mohon tunggu.")
					
					if err := doBackupAndSend(context.Background()); err != nil {
						errorMsg := fmt.Sprintf("❌ Backup gagal: %v", err)
						sendText(u.Message.Chat.ID, errorMsg)
						fmt.Printf("[ERR] Manual backup gagal: %v\n", err)
						return
					}
					
					sendText(u.Message.Chat.ID, "✅ Backup selesai dan berhasil dikirim ke grup.")
					fmt.Println("[OK] Manual backup berhasil")
				}()
				
			case strings.HasPrefix(text, "/chatid"):
				chatIDMsg := fmt.Sprintf("💬 Chat ID: %d\nTipe: %s", u.Message.Chat.ID, u.Message.Chat.Type)
				sendText(u.Message.Chat.ID, chatIDMsg)
				
			case strings.HasPrefix(text, "/help"):
				helpMsg := `📋 *Perintah yang tersedia:*
				
/backup - Melakukan backup tabel klinik_apps
/chatid - Menampilkan Chat ID
/help - Menampilkan bantuan ini

ℹ️ Bot ini akan backup tabel: ` + backupTables
				sendText(u.Message.Chat.ID, helpMsg)
			}
		}
	}
}

func sendText(chat int64, text string) {
	client := &http.Client{ Timeout: 15 * time.Second }
	url := fmt.Sprintf(telegramAPI, botToken, "sendMessage")
	
	payload := fmt.Sprintf("chat_id=%d&text=%s&parse_mode=Markdown", chat, urlEncode(text))
	req, err := http.NewRequest("POST", url, strings.NewReader(payload))
	if err != nil {
		fmt.Printf("[WARN] Error creating sendText request: %v\n", err)
		return
	}
	
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("[WARN] Error sending message: %v\n", err)
		return
	}
	resp.Body.Close()
}

func urlEncode(s string) string { 
	s = strings.ReplaceAll(s, "&", "%26")
	s = strings.ReplaceAll(s, "+", "%2B")
	s = strings.ReplaceAll(s, "#", "%23")
	s = strings.ReplaceAll(s, "=", "%3D")
	return s
}

func doBackupAndSend(ctx context.Context) error {
	// Nama file dengan info tabel
	stamp := time.Now().Format("20060102_150405")
	fname := fmt.Sprintf("%s_%s_%s.sql.gz", mysqlDB, strings.ReplaceAll(backupTables, ",", "_"), stamp)
	fpath := filepath.Join(backupDir, fname)

	fmt.Printf("[INFO] Memulai backup ke file: %s\n", fname)

	// Jalankan mysqldump dengan tabel spesifik -> gzip 
	tables := strings.Fields(strings.ReplaceAll(backupTables, ",", " "))
	tableArgs := strings.Join(tables, " ")
	
	dumpCmd := fmt.Sprintf("mysqldump -h %s -P %s -u %s --single-transaction --quick --routines --triggers --events --set-gtid-purged=OFF %s %s | gzip -c > %s",
		shEscape(mysqlHost), 
		shEscape(mysqlPort), 
		shEscape(mysqlUser), 
		shEscape(mysqlDB), 
		tableArgs,  // tabel spesifik
		shEscape(fpath))

	cmd := exec.CommandContext(ctx, "bash", "-c", dumpCmd)
	
	// Set environment untuk password MySQL
	env := os.Environ()
	if mysqlPass != "" {
		env = append(env, "MYSQL_PWD="+mysqlPass)
	}
	cmd.Env = env

	fmt.Printf("[INFO] Menjalankan: mysqldump untuk tabel %s\n", backupTables)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mysqldump error: %v, output: %s", err, string(out))
	}

	// Cek ukuran file
	fileInfo, err := os.Stat(fpath)
	if err != nil {
		return fmt.Errorf("tidak dapat membaca info file backup: %v", err)
	}
	
	fileSizeMB := float64(fileInfo.Size()) / (1024 * 1024)
	fmt.Printf("[INFO] Backup selesai, ukuran file: %.2f MB\n", fileSizeMB)

	// Kirim ke Telegram sebagai dokumen
	targetChatID := parseChatID(chatID)
	if err := sendDocument(fpath, fname, targetChatID); err != nil {
		return fmt.Errorf("gagal mengirim ke Telegram: %v", err)
	}

	fmt.Printf("[OK] Backup berhasil dikirim ke Telegram (Chat ID: %s)\n", chatID)
	return nil
}

func shEscape(s string) string {
	// Escape untuk shell arguments
	if strings.Contains(s, " ") || strings.Contains(s, "'") || strings.Contains(s, "\"") {
		return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "'\"'\"'"))
	}
	return s
}

func sendDocument(path, displayName string, targetChatID int64) error {
	file, err := os.Open(path)
	if err != nil { 
		return fmt.Errorf("tidak dapat membuka file: %v", err)
	}
	defer file.Close()

	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	_ = w.WriteField("chat_id", strconv.FormatInt(targetChatID, 10))
	_ = w.WriteField("disable_content_type_detection", "true")
	
	caption := fmt.Sprintf("📊 *MySQL Backup*\n\n" +
		"🗃 Database: `%s`\n" +
		"📋 Tabel: `%s`\n" +
		"📅 Waktu: %s\n" +
		"📁 File: `%s`",
		mysqlDB,
		backupTables,
		time.Now().Format("2006-01-02 15:04:05"),
		displayName)
	
	_ = w.WriteField("caption", caption)
	_ = w.WriteField("parse_mode", "Markdown")

	fw, err := w.CreateFormFile("document", displayName)
	if err != nil { 
		return fmt.Errorf("tidak dapat membuat form file: %v", err)
	}
	
	if _, err := io.Copy(fw, file); err != nil { 
		return fmt.Errorf("tidak dapat copy file: %v", err)
	}
	w.Close()

	client := &http.Client{ Timeout: 10 * time.Minute }
	url := fmt.Sprintf(telegramAPI, botToken, "sendDocument")
	
	req, err := http.NewRequest("POST", url, &b)
	if err != nil {
		return fmt.Errorf("tidak dapat membuat request: %v", err)
	}
	
	req.Header.Set("Content-Type", w.FormDataContentType())
	
	resp, err := client.Do(req)
	if err != nil { 
		return fmt.Errorf("request gagal: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("telegram API error (status %d): %s", resp.StatusCode, string(body))
	}
	
	return nil
}

func applyRetention() error {
	days, _ := strconv.Atoi(retentionDays)
	if days <= 0 { 
		fmt.Println("[INFO] Retention dinonaktifkan (RETENTION_DAYS <= 0)")
		return nil 
	}
	
	cutoff := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	fmt.Printf("[INFO] Membersihkan backup yang lebih lama dari %d hari (sebelum %s)\n", 
		days, cutoff.Format("2006-01-02 15:04:05"))
	
	entries, err := os.ReadDir(backupDir)
	if err != nil { 
		return fmt.Errorf("tidak dapat membaca direktori backup: %v", err)
	}
	
	deleted := 0
	for _, e := range entries {
		if e.IsDir() { continue }
		if !strings.HasSuffix(e.Name(), ".sql.gz") { continue }
		
		p := filepath.Join(backupDir, e.Name())
		info, err := os.Stat(p)
		if err != nil { 
			fmt.Printf("[WARN] Tidak dapat stat file %s: %v\n", e.Name(), err)
			continue 
		}
		
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(p); err != nil {
				fmt.Printf("[WARN] Tidak dapat menghapus %s: %v\n", e.Name(), err)
			} else {
				fmt.Printf("[INFO] Menghapus backup lama: %s\n", e.Name())
				deleted++
			}
		}
	}
	
	fmt.Printf("[INFO] Retention selesai, %d file dihapus\n", deleted)
	return nil
}

// Utility: random string (untuk keperluan masa depan)
func randString(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	rand.Read(b)
	for i := range b { 
		b[i] = charset[int(b[i])%len(charset)] 
	}
	return string(b)
}