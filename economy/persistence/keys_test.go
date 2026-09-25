package persistence

import "testing"

func TestKeyBuildersProductionNamespace(t *testing.T) {
	// Empty namespace ("") is production. Keys match LAYER_5_PERSISTENCE §7 verbatim.
	sessionID := "sess-1"
	accountID := "acct-42"
	instrumentID := "ACME"
	orderID := "ord-999"

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"cash", cashKey("", sessionID, accountID), "l5:snap:sess-1:acct:acct-42:cash"},
		{"pos", posKey("", sessionID, accountID, instrumentID), "l5:snap:sess-1:acct:acct-42:pos:ACME"},
		{"pos-prefix", posPrefix("", sessionID, accountID), "l5:snap:sess-1:acct:acct-42:pos:*"},
		{"resv", resvKey("", sessionID, accountID, orderID), "l5:snap:sess-1:acct:acct-42:resv:ord-999"},
		{"resv-prefix", resvPrefix("", sessionID, accountID), "l5:snap:sess-1:acct:acct-42:resv:*"},
		{"buf", bufKey(""), "l5:buf:trades"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			if testCase.got != testCase.want {
				subtest.Errorf("got %q, want %q", testCase.got, testCase.want)
			}
		})
	}
}

func TestKeyBuildersWithNamespace(t *testing.T) {
	// Non-empty namespace prefixes every key with "{ns}:" so integration tests
	// can isolate their keyspace.
	namespace := "test_run_7"
	sessionID := "sess-1"
	accountID := "acct-42"

	if got := cashKey(namespace, sessionID, accountID); got != "test_run_7:l5:snap:sess-1:acct:acct-42:cash" {
		t.Errorf("namespaced cashKey: got %q", got)
	}
	if got := bufKey(namespace); got != "test_run_7:l5:buf:trades" {
		t.Errorf("namespaced bufKey: got %q", got)
	}
	if got := posPrefix(namespace, sessionID, accountID); got != "test_run_7:l5:snap:sess-1:acct:acct-42:pos:*" {
		t.Errorf("namespaced posPrefix: got %q", got)
	}
}

func TestKeyBuildersEmptyIdentifiersDoNotCollapse(t *testing.T) {
	// Empty sessionID / accountID / instrumentID / orderID are caller bugs, but
	// the key builder must not silently produce a colliding key — it should
	// leave the empty segment as an empty component so callers see the malformed
	// key in a log/dashboard.
	if got := cashKey("", "", ""); got != "l5:snap::acct::cash" {
		t.Errorf("empty ids: got %q", got)
	}
}
