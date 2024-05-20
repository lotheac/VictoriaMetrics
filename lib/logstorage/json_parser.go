package logstorage

import (
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastjson"
)

// JSONParser parses a single JSON log message into Fields.
//
// See https://docs.victoriametrics.com/VictoriaLogs/keyConcepts.html#data-model
//
// Use GetParser() for obtaining the parser.
type JSONParser struct {
	// Fields contains the parsed JSON line after Parse() call
	//
	// The Fields are valid until the next call to ParseLogMessage()
	// or until the parser is returned to the pool with PutParser() call.
	Fields []Field

	// p is used for fast JSON parsing
	p fastjson.Parser

	// buf is used for holding the backing data for Fields
	buf []byte

	// prefixBuf is used for holding the current key prefix
	// when it is composed from multiple keys.
	prefixBuf []byte
}

func (p *JSONParser) reset() {
	p.resetNobuf()

	p.buf = p.buf[:0]
}

func (p *JSONParser) resetNobuf() {
	clear(p.Fields)
	p.Fields = p.Fields[:0]

	p.prefixBuf = p.prefixBuf[:0]
}

// GetJSONParser returns JSONParser ready to parse JSON lines.
//
// Return the parser to the pool when it is no longer needed by calling PutJSONParser().
func GetJSONParser() *JSONParser {
	v := parserPool.Get()
	if v == nil {
		return &JSONParser{}
	}
	return v.(*JSONParser)
}

// PutJSONParser returns the parser to the pool.
//
// The parser cannot be used after returning to the pool.
func PutJSONParser(p *JSONParser) {
	p.reset()
	parserPool.Put(p)
}

var parserPool sync.Pool

// ParseLogMessageNoResetBuf parses the given JSON log message msg into p.Fields.
//
// It adds the given prefix to all the parsed field names.
//
// The p.Fields remains valid until the next call to PutJSONParser().
func (p *JSONParser) ParseLogMessageNoResetBuf(msg, prefix string) error {
	return p.parseLogMessage(msg, prefix, false)
}

// ParseLogMessage parses the given JSON log message msg into p.Fields.
//
// It adds the given prefix to all the parsed field names.
//
// The p.Fields remains valid until the next call to ParseLogMessage() or PutJSONParser().
func (p *JSONParser) ParseLogMessage(msg []byte, prefix string) error {
	msgStr := bytesutil.ToUnsafeString(msg)
	return p.parseLogMessage(msgStr, prefix, true)
}

func (p *JSONParser) parseLogMessage(msg, prefix string, resetBuf bool) error {
	v, err := p.p.Parse(msg)
	if err != nil {
		return fmt.Errorf("cannot parse json: %w", err)
	}
	if t := v.Type(); t != fastjson.TypeObject {
		return fmt.Errorf("expecting json dictionary; got %s", t)
	}
	if resetBuf {
		p.reset()
	} else {
		p.resetNobuf()
	}
	p.prefixBuf = append(p.prefixBuf[:0], prefix...)
	p.Fields, p.buf, p.prefixBuf = appendLogFields(p.Fields, p.buf, p.prefixBuf, v)
	return nil
}

// RenameField renames field with the oldName to newName in p.Fields
func (p *JSONParser) RenameField(oldName, newName string) {
	if oldName == "" {
		return
	}
	fields := p.Fields
	for i := range fields {
		f := &fields[i]
		if f.Name == oldName {
			f.Name = newName
			return
		}
	}
}

func appendLogFields(dst []Field, dstBuf, prefixBuf []byte, v *fastjson.Value) ([]Field, []byte, []byte) {
	o := v.GetObject()
	o.Visit(func(k []byte, v *fastjson.Value) {
		t := v.Type()
		switch t {
		case fastjson.TypeNull:
			// Skip nulls
		case fastjson.TypeObject:
			// Flatten nested JSON objects.
			// For example, {"foo":{"bar":"baz"}} is converted to {"foo.bar":"baz"}
			prefixLen := len(prefixBuf)
			prefixBuf = append(prefixBuf, k...)
			prefixBuf = append(prefixBuf, '.')
			dst, dstBuf, prefixBuf = appendLogFields(dst, dstBuf, prefixBuf, v)
			prefixBuf = prefixBuf[:prefixLen]
		case fastjson.TypeArray, fastjson.TypeNumber, fastjson.TypeTrue, fastjson.TypeFalse:
			// Convert JSON arrays, numbers, true and false values to their string representation
			dstBufLen := len(dstBuf)
			dstBuf = v.MarshalTo(dstBuf)
			value := dstBuf[dstBufLen:]
			dst, dstBuf = appendLogField(dst, dstBuf, prefixBuf, k, value)
		case fastjson.TypeString:
			// Decode JSON strings
			dstBufLen := len(dstBuf)
			dstBuf = append(dstBuf, v.GetStringBytes()...)
			value := dstBuf[dstBufLen:]
			dst, dstBuf = appendLogField(dst, dstBuf, prefixBuf, k, value)
		default:
			logger.Panicf("BUG: unexpected JSON type: %s", t)
		}
	})
	return dst, dstBuf, prefixBuf
}

func appendLogField(dst []Field, dstBuf, prefixBuf, k, value []byte) ([]Field, []byte) {
	dstBufLen := len(dstBuf)
	dstBuf = append(dstBuf, prefixBuf...)
	dstBuf = append(dstBuf, k...)
	name := dstBuf[dstBufLen:]

	dst = append(dst, Field{
		Name:  bytesutil.ToUnsafeString(name),
		Value: bytesutil.ToUnsafeString(value),
	})
	return dst, dstBuf
}
