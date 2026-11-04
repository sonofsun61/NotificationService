package mailer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"
)

var ErrInvalidMessage = errors.New("invalid message")

type Mailer struct {
	addr string
	from string
}

func NewMailer(addr, from string) *Mailer {
	return &Mailer{
		addr: addr,
		from: from,
	}
}

func (m *Mailer) Send(ctx context.Context, to, subject, body string) error {
	msg, err := buildMessage(m.from, to, subject, body, time.Now())
	if err != nil {
		return err
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", m.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to smtp server: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("failed to set smtp deadline: %w", err)
		}
	}
	host, _, err := net.SplitHostPort(m.addr)
	if err != nil {
		return fmt.Errorf("invalid smtp address: %w", err)
	}
	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("failed to start smtp session: %w", err)
	}
	defer client.Close()
	if err := client.Mail(m.from); err != nil {
		return fmt.Errorf("smtp MAIL FROM failed: %w", err)
	}
	if err := client.Rcpt(to); err != nil {
		return fmt.Errorf("smtp RCPT TO failed: %w", err)
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp DATA failed: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to finish message: %w", err)
	}
	if err := client.Quit(); err != nil {
		return fmt.Errorf("smtp QUIT failed: %w", err)
	}
	return nil
}

func buildMessage(from, to, subject, body string, now time.Time) ([]byte, error) {
	addr, err := mail.ParseAddress(to)
	if err != nil || addr.Address != to {
		return nil, fmt.Errorf("%w: bad recipient %q", ErrInvalidMessage, to)
	}
	if strings.ContainsAny(subject, "\r\n") {
		return nil, fmt.Errorf("%w: subject contains line breaks", ErrInvalidMessage)
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "From: %s\r\n", from)
	fmt.Fprintf(&buf, "To: %s\r\n", to)
	fmt.Fprintf(&buf, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	fmt.Fprintf(&buf, "Date: %s\r\n", now.Format(time.RFC1123Z))
	buf.WriteString("MIME-Version: 1.0\r\n")
	buf.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	buf.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	buf.WriteString("\r\n")
	buf.WriteString(body)
	return buf.Bytes(), nil
}
