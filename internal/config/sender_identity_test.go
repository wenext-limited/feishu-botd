package config

import "testing"

func TestSenderIdentityGrantIsExplicitAndIndependent(t *testing.T) {
	for _, tc := range []struct {
		entry string
		want  bool
	}{{"", false}, {`,"allow_sender_identity":false`, false}, {`,"allow_sender_identity":true`, true}} {
		cfg := writeSenderEmailProviderConfig(t, tc.entry)
		if cfg.ProviderAllowsSenderIdentity("fixture-agent") != tc.want {
			t.Fatal("identity grant did not match configuration")
		}
		if cfg.ProviderAllowsSenderIdentity("unknown") || cfg.ProviderAllowsSenderEmail("fixture-agent") {
			t.Fatal("identity widened another grant")
		}
	}
}
