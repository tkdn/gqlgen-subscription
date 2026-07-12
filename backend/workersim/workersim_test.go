package workersim

import "testing"

func TestResultStatus(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "job-1", want: "COMPLETED"},
		{name: "fail-job-1", want: "FAILED"},
		{name: "fail-", want: "FAILED"},
		{name: "job-fail-1", want: "COMPLETED"},
	}

	for _, tt := range tests {
		if got := resultStatus(tt.name); got != tt.want {
			t.Errorf("resultStatus(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}
