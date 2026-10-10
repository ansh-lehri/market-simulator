package marketdata

import "testing"

// tzFor must resolve the known markets to their IANA zones and miss on
// everything else. The HTTP handler maps a miss to 400 bad params, so a
// regression here (e.g. the init-time LoadLocation silently failing)
// would surface as every request 400-ing — these guard that.

func TestTzFor_INResolvesToAsiaKolkata(t *testing.T) {
	location, ok := tzFor("IN")
	if !ok {
		t.Fatal("tzFor(IN): got (nil,false), want Asia/Kolkata")
	}
	if location == nil {
		t.Fatal("tzFor(IN): location is nil despite ok=true")
	}
	if got := location.String(); got != "Asia/Kolkata" {
		t.Errorf("tzFor(IN).String(): got %q, want %q", got, "Asia/Kolkata")
	}
}

func TestTzFor_USResolvesToAmericaNewYork(t *testing.T) {
	location, ok := tzFor("US")
	if !ok {
		t.Fatal("tzFor(US): got (nil,false), want America/New_York")
	}
	if location == nil {
		t.Fatal("tzFor(US): location is nil despite ok=true")
	}
	if got := location.String(); got != "America/New_York" {
		t.Errorf("tzFor(US).String(): got %q, want %q", got, "America/New_York")
	}
}

func TestTzFor_UnknownMarketReturnsNotOK(t *testing.T) {
	cases := []string{
		"",     // empty — client omitted the param
		"GB",   // never-allowlisted market
		"in",   // case-sensitive, lowercase is a mistake
		"XYZ",  // nonsense
	}
	for _, market := range cases {
		t.Run(market, func(t *testing.T) {
			location, ok := tzFor(market)
			if ok {
				t.Errorf("tzFor(%q): got ok=true, want false", market)
			}
			if location != nil {
				t.Errorf("tzFor(%q): got non-nil location on miss", market)
			}
		})
	}
}
