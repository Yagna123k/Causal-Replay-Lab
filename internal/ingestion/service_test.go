package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
)

func ts() *time.Time { t := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC); return &t }

func validReq() Request {
	return Request{
		EventID:        "evt-1",
		TransactionID:  "txn-1",
		EventType:      "TRANSACTION_CREATED",
		Payload:        json.RawMessage(`{"amount": 100, "currency":"USD"}`),
		SequenceNumber: 1,
		CreatedAt:      ts(),
	}
}

func fieldsOf(t *testing.T, err error) map[string]bool {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
	m := map[string]bool{}
	for _, f := range ve.Fields {
		m[f.Field] = true
	}
	return m
}

func TestNormalizeValid(t *testing.T) {
	ev, err := Normalize(validReq())
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Metadata) != `{}` || ev.SchemaVersion != CurrentSchemaVersion || ev.PayloadHash == "" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if string(ev.Payload) != `{"amount":100,"currency":"USD"}` {
		t.Fatalf("payload not canonical: %s", ev.Payload)
	}
}

func TestNormalizeInvalid(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Request)
		field  string
	}{
		"missing event id":  {func(r *Request) { r.EventID = "" }, "event_id"},
		"long txn id":       {func(r *Request) { r.TransactionID = string(make([]byte, 129)) }, "transaction_id"},
		"missing type":      {func(r *Request) { r.EventType = "" }, "event_type"},
		"lowercase type":    {func(r *Request) { r.EventType = "created" }, "event_type"},
		"zero sequence":     {func(r *Request) { r.SequenceNumber = 0 }, "sequence_number"},
		"negative sequence": {func(r *Request) { r.SequenceNumber = -3 }, "sequence_number"},
		"missing created":   {func(r *Request) { r.CreatedAt = nil }, "created_at"},
		"missing payload":   {func(r *Request) { r.Payload = nil }, "payload"},
		"array payload":     {func(r *Request) { r.Payload = json.RawMessage(`[1]`) }, "payload"},
		"null payload":      {func(r *Request) { r.Payload = json.RawMessage(`null`) }, "payload"},
		"array metadata":    {func(r *Request) { r.Metadata = json.RawMessage(`[1]`) }, "metadata"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := validReq()
			tc.mutate(&r)
			_, err := Normalize(r)
			if !fieldsOf(t, err)[tc.field] {
				t.Fatalf("expected error on %s, got %v", tc.field, err)
			}
		})
	}
}

func TestNormalizeReportsAllFields(t *testing.T) {
	_, err := Normalize(Request{})
	m := fieldsOf(t, err)
	for _, f := range []string{"event_id", "transaction_id", "event_type", "sequence_number", "created_at", "payload"} {
		if !m[f] {
			t.Errorf("missing error for %s", f)
		}
	}
}

func TestHashIgnoresKeyOrderAndMetadata(t *testing.T) {
	a := validReq()
	b := validReq()
	b.Payload = json.RawMessage(`{"currency":"USD","amount":100}`)
	b.Metadata = json.RawMessage(`{"source":"retry"}`)
	ea, _ := Normalize(a)
	eb, _ := Normalize(b)
	if ea.PayloadHash != eb.PayloadHash {
		t.Fatal("equal logical content should hash equally")
	}
	c := validReq()
	c.Payload = json.RawMessage(`{"amount":101,"currency":"USD"}`)
	ec, _ := Normalize(c)
	if ea.PayloadHash == ec.PayloadHash {
		t.Fatal("different payload should hash differently")
	}
	d := validReq()
	d.SequenceNumber = 2
	ed, _ := Normalize(d)
	if ea.PayloadHash == ed.PayloadHash {
		t.Fatal("different sequence should hash differently")
	}
}

type fakeStore struct {
	got eventstore.Event
	err error
}

func (f *fakeStore) Insert(_ context.Context, ev eventstore.Event) (eventstore.Event, bool, error) {
	f.got = ev
	return ev, f.err == nil, f.err
}

func TestServiceIngest(t *testing.T) {
	st := &fakeStore{}
	svc := NewService(st)
	res, err := svc.Ingest(context.Background(), validReq())
	if err != nil || res.Duplicate || st.got.EventID != "evt-1" {
		t.Fatalf("res=%+v err=%v", res, err)
	}

	st.err = eventstore.ErrPayloadConflict
	if _, err := svc.Ingest(context.Background(), validReq()); !errors.Is(err, eventstore.ErrPayloadConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	st2 := &fakeStore{}
	if _, err := NewService(st2).Ingest(context.Background(), Request{}); err == nil || st2.got.EventID != "" {
		t.Fatal("invalid request must not reach the store")
	}
}
