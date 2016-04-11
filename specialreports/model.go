package specialreports

type SpecialReport struct {
	UUID          string `json:"uuid"`
	CanonicalName string `json:"canonicalName"`
	TmeIdentifier string `json:"tmeIdentifier,omitempty"`
	Type          string `json:"type,omitempty"`
}

type SpecialReportLink struct {
	ApiUrl string `json:"apiUrl"`
}
