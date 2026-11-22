package protocol

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// WSClient provides synthetic WebSocket load generation with RFC 6455 frame construction.
type WSClient struct {
	config  TargetConfig
	mu      sync.Mutex
	conn    net.Conn
	reader  *bufio.Reader
	secKey  string
	closing bool
}

// NewWSClient instantiates a synthetic WebSocket client.
func NewWSClient(cfg TargetConfig) *WSClient {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	return &WSClient{
		config: cfg,
	}
}

func (c *WSClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	u, err := url.Parse(c.config.URL)
	if err != nil {
		return err
	}

	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	dialer := &net.Dialer{Timeout: c.config.Timeout}
	var rawConn net.Conn
	if u.Scheme == "wss" {
		rawConn, err = tls.DialWithDialer(dialer, "tcp", host, &tls.Config{
			InsecureSkipVerify: c.config.TLSInsecure,
		})
	} else {
		rawConn, err = dialer.Dial("tcp", host)
	}
	if err != nil {
		return err
	}

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		_ = rawConn.Close()
		return fmt.Errorf("generate websocket handshake nonce: %w", err)
	}
	c.secKey = base64.StdEncoding.EncodeToString(nonce)

	path := u.RequestURI()
	if path == "" {
		path = "/"
	}

	req := fmt.Sprintf("GET %s HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\n"+
		"Sec-WebSocket-Version: 13\r\n", path, u.Host, c.secKey)

	for k, v := range c.config.Headers {
		req += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	req += "\r\n"

	_ = rawConn.SetDeadline(time.Now().Add(c.config.Timeout))
	if _, err := rawConn.Write([]byte(req)); err != nil {
		_ = rawConn.Close()
		return err
	}

	reader := bufio.NewReader(rawConn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		_ = rawConn.Close()
		return err
	}
	if err := validateHandshakeResponse(statusLine, reader, c.secKey); err != nil {
		_ = rawConn.Close()
		return err
	}

	_ = rawConn.SetDeadline(time.Time{})
	c.conn = rawConn
	c.reader = reader
	return nil
}

// validateHandshakeResponse verifies the HTTP upgrade status and RFC 6455 accept key.
func validateHandshakeResponse(statusLine string, reader *bufio.Reader, key string) error {
	fields := strings.Fields(statusLine)
	if len(fields) < 2 || fields[1] != "101" {
		return fmt.Errorf("websocket handshake failed: %s", strings.TrimSpace(statusLine))
	}

	accept := ""
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read websocket handshake headers: %w", err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(name), "Sec-WebSocket-Accept") {
			accept = strings.TrimSpace(value)
		}
	}

	if accept != computeWSKeyAccept(key) {
		return fmt.Errorf("websocket handshake failed: invalid Sec-WebSocket-Accept header")
	}
	return nil
}

// computeWSKeyAccept generates the expected Sec-WebSocket-Accept header value per RFC 6455
func computeWSKeyAccept(key string) string {
	const magicGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	h := sha1.New()
	h.Write([]byte(key + magicGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// Execute transmits a masked text or binary WebSocket frame and measures round-trip ack.
func (c *WSClient) Execute(ctx context.Context) Result {
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return Result{
				Timestamp: time.Now(),
				Err:       err,
			}
		}
	}

	payload := c.config.Body
	if len(payload) == 0 {
		payload = []byte("turbostorm-ping")
	}

	frame, err := encodeWSFrame(0x1, payload) // 0x1 text frame with a fresh client mask
	if err != nil {
		return Result{Timestamp: time.Now(), Err: fmt.Errorf("encode websocket frame: %w", err)}
	}
	start := time.Now()

	c.mu.Lock()
	conn := c.conn
	reader := c.reader
	c.mu.Unlock()

	if conn == nil {
		return Result{
			Timestamp: start,
			Err:       fmt.Errorf("connection not available"),
		}
	}

	_ = conn.SetDeadline(time.Now().Add(c.config.Timeout))
	nSent, err := conn.Write(frame)
	if err != nil {
		c.resetConn()
		return Result{
			Timestamp: start,
			Duration:  time.Since(start),
			BytesSent: int64(nSent),
			Err:       err,
		}
	}

	// Read reply frame
	header := make([]byte, 2)
	_, err = io.ReadFull(reader, header)
	duration := time.Since(start)
	if err != nil {
		c.resetConn()
		return Result{
			Timestamp: start,
			Duration:  duration,
			BytesSent: int64(nSent),
			Err:       err,
		}
	}

	payloadLen := int(header[1] & 0x7F)
	if payloadLen == 126 {
		ext := make([]byte, 2)
		_, _ = io.ReadFull(reader, ext)
		payloadLen = int(ext[0])<<8 | int(ext[1])
	} else if payloadLen == 127 {
		ext := make([]byte, 8)
		_, _ = io.ReadFull(reader, ext)
		payloadLen = int(ext[4])<<24 | int(ext[5])<<16 | int(ext[6])<<8 | int(ext[7])
	}

	recvBuf := make([]byte, payloadLen)
	_, _ = io.ReadFull(reader, recvBuf)

	return Result{
		Timestamp:  start,
		Duration:   duration,
		StatusCode: 101,
		BytesSent:  int64(nSent),
		BytesRecv:  int64(2 + payloadLen),
	}
}

func encodeWSFrame(opcode byte, payload []byte) ([]byte, error) {
	length := len(payload)
	var header []byte
	header = append(header, 0x80|opcode) // FIN bit + opcode

	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return nil, fmt.Errorf("generate websocket frame mask: %w", err)
	}

	if length <= 125 {
		header = append(header, 0x80|byte(length))
	} else if length <= 65535 {
		header = append(header, 0x80|126, byte(length>>8), byte(length))
	} else {
		header = append(header, 0x80|127, 0, 0, 0, 0,
			byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	}

	header = append(header, mask...)
	masked := make([]byte, length)
	for i := 0; i < length; i++ {
		masked[i] = payload[i] ^ mask[i%4]
	}

	return append(header, masked...), nil
}

func (c *WSClient) resetConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}

// Close gracefully closes the WebSocket connection.
func (c *WSClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closing = true
	if c.conn != nil {
		// send close frame (0x8)
		closeFrame, frameErr := encodeWSFrame(0x8, []byte{})
		if frameErr == nil {
			_ = c.conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
			_, _ = c.conn.Write(closeFrame)
		}
		err := c.conn.Close()
		if frameErr != nil {
			return frameErr
		}
		c.conn = nil
		c.reader = nil
		return err
	}
	return nil
}
