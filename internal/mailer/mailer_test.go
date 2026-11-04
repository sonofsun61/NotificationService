package mailer

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSMTPServer struct {
	addr string

	mu    sync.Mutex
	from  string
	to    string
	data  string
	conns []net.Conn
}

func startFakeSMTPServer(t *testing.T, greet bool) *fakeSMTPServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &fakeSMTPServer{addr: lis.Addr().String()}
	t.Cleanup(func() {
		lis.Close()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, conn := range s.conns {
			conn.Close()
		}
	})
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			if greet {
				go s.serve(conn)
			}
		}
	}()
	return s
}

func (s *fakeSMTPServer) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	reply := func(line string) { conn.Write([]byte(line + "\r\n")) }
	reply("220 fake ready")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		cmd := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			reply("250 fake")
		case strings.HasPrefix(cmd, "MAIL FROM:"):
			s.mu.Lock()
			s.from = line[len("MAIL FROM:"):]
			s.mu.Unlock()
			reply("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO:"):
			s.mu.Lock()
			s.to = line[len("RCPT TO:"):]
			s.mu.Unlock()
			reply("250 ok")
		case cmd == "DATA":
			reply("354 go ahead")
			var data strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				data.WriteString(l)
			}
			s.mu.Lock()
			s.data = data.String()
			s.mu.Unlock()
			reply("250 queued")
		case cmd == "QUIT":
			reply("221 bye")
			return
		default:
			reply("500 unknown")
		}
	}
}

func (s *fakeSMTPServer) received() (from, to, data string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.from, s.to, s.data
}

func TestSend_Success(t *testing.T) {
	server := startFakeSMTPServer(t, true)
	m := NewMailer(server.addr, "noreply@shop.local")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := m.Send(ctx, "user@example.com", "Welcome!", "Hello, Ivan")
	require.NoError(t, err)

	from, to, data := server.received()
	assert.Equal(t, "<noreply@shop.local>", from)
	assert.Equal(t, "<user@example.com>", to)
	assert.Contains(t, data, "To: user@example.com\r\n")
	assert.Contains(t, data, "Subject: Welcome!\r\n")
	assert.True(t, strings.HasSuffix(data, "\r\n\r\nHello, Ivan\r\n"))
}

func TestSend_ServerUnavailable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	lis.Close()
	m := NewMailer(addr, "noreply@shop.local")

	err = m.Send(context.Background(), "user@example.com", "Welcome!", "Hello")
	assert.ErrorContains(t, err, "failed to connect to smtp server")
}

func TestSend_ServerHangs(t *testing.T) {
	server := startFakeSMTPServer(t, false)
	m := NewMailer(server.addr, "noreply@shop.local")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := m.Send(ctx, "user@example.com", "Welcome!", "Hello")
	assert.Error(t, err)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestSend_InvalidMessage(t *testing.T) {
	m := NewMailer("127.0.0.1:1", "noreply@shop.local")

	err := m.Send(context.Background(), "not-an-email", "Welcome!", "Hello")
	assert.ErrorIs(t, err, ErrInvalidMessage)
}

func TestBuildMessage(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

	msg, err := buildMessage("noreply@shop.local", "user@example.com", "Привет", "Body", now)
	require.NoError(t, err)

	text := string(msg)
	assert.Contains(t, text, "From: noreply@shop.local\r\n")
	assert.Contains(t, text, "To: user@example.com\r\n")
	assert.Contains(t, text, "Subject: =?utf-8?q?")
	assert.Contains(t, text, "Date: Wed, 04 Nov 2026 12:00:00 +0000\r\n")
	assert.Contains(t, text, "Content-Type: text/plain; charset=UTF-8\r\n")
	assert.True(t, strings.HasSuffix(text, "\r\n\r\nBody"))
}

func TestBuildMessage_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		to      string
		subject string
	}{
		{"empty recipient", "", "Welcome!"},
		{"recipient with display name", "Ivan <user@example.com>", "Welcome!"},
		{"header injection in recipient", "user@example.com\r\nBcc: victim@example.com", "Welcome!"},
		{"header injection in subject", "user@example.com", "Hi\r\nBcc: victim@example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildMessage("noreply@shop.local", tt.to, tt.subject, "Body", time.Now())
			assert.ErrorIs(t, err, ErrInvalidMessage)
		})
	}
}
