package specialreports

import (
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	specialreportUUID    = "12345"
	newSpecialReportUUID = "123456"
	tmeID                = "TME_ID"
	newTmeID             = "NEW_TME_ID"
	prefLabel            = "Test"
	specialCharPrefLabel = "Test 'special chars"
)

var defaultTypes = []string{"Thing", "Concept", "Classification", "SpecialReport"}

func TestConnectivityCheck(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	err := specialreportsDriver.Check()
	assert.NoError(t, err, "Unexpected error on connectivity check")
}

func TestPrefLabelIsCorrectlyWritten(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	err := specialreportsDriver.Write(specialreportToWrite)
	assert.NoError(t, err, "ERROR happened during write time")

	storedSpecialReport, found, err := specialreportsDriver.Read(specialreportUUID)
	assert.NoError(t, err, "ERROR happened during read time")
	assert.Equal(t, true, found)
	assert.NotEmpty(t, storedSpecialReport)

	assert.Equal(t, prefLabel, storedSpecialReport.(SpecialReport).PrefLabel, "PrefLabel should be "+prefLabel)
	cleanUp(t, specialreportUUID, specialreportsDriver)
}

func TestPrefLabelSpecialCharactersAreHandledByCreate(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(t, specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")

	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes
	//check if specialreportToWrite is the same with the one inside the DB
	readSpecialReportForUUIDAndCheckFieldsMatch(t, specialreportsDriver, specialreportUUID, specialreportToWrite)
	cleanUp(t, specialreportUUID, specialreportsDriver)
}

func TestCreateCompleteSpecialReportWithPropsAndIdentifiers(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(t, specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")

	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes
	//check if specialreportToWrite is the same with the one inside the DB
	readSpecialReportForUUIDAndCheckFieldsMatch(t, specialreportsDriver, specialreportUUID, specialreportToWrite)
	cleanUp(t, specialreportUUID, specialreportsDriver)
}

func TestUpdateWillRemovePropertiesAndIdentifiersNoLongerPresent(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	allAlternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: allAlternativeIdentifiers}

	assert.NoError(t, specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")
	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes
	readSpecialReportForUUIDAndCheckFieldsMatch(t, specialreportsDriver, specialreportUUID, specialreportToWrite)

	tmeAlternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	updatedSpecialReport := SpecialReport{UUID: specialreportUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: tmeAlternativeIdentifiers}

	assert.NoError(t, specialreportsDriver.Write(updatedSpecialReport), "Failed to write updated specialreport")
	//add default types that will be automatically added by the writer
	updatedSpecialReport.Types = defaultTypes
	readSpecialReportForUUIDAndCheckFieldsMatch(t, specialreportsDriver, specialreportUUID, updatedSpecialReport)

	cleanUp(t, specialreportUUID, specialreportsDriver)
}

func TestDelete(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	specialreportToDelete := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(t, specialreportsDriver.Write(specialreportToDelete), "Failed to write specialreport")

	found, err := specialreportsDriver.Delete(specialreportUUID)
	assert.True(t, found, "Didn't manage to delete specialreport for uuid %", specialreportUUID)
	assert.NoError(t, err, "Error deleting specialreport for uuid %s", specialreportUUID)

	p, found, err := specialreportsDriver.Read(specialreportUUID)

	assert.Equal(t, SpecialReport{}, p, "Found specialreport %s who should have been deleted", p)
	assert.False(t, found, "Found specialreport for uuid %s who should have been deleted", specialreportUUID)
	assert.NoError(t, err, "Error trying to find specialreport for uuid %s", specialreportUUID)
}

func TestCount(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIds := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	specialreportOneToCount := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIds}

	assert.NoError(t, specialreportsDriver.Write(specialreportOneToCount), "Failed to write specialreport")

	nr, err := specialreportsDriver.Count()
	assert.Equal(t, 1, nr, "Should be 1 specialreports in DB - count differs")
	assert.NoError(t, err, "An unexpected error occurred during count")

	newAlternativeIds := alternativeIdentifiers{TME: []string{newTmeID}, UUIDS: []string{newSpecialReportUUID}}
	specialreportTwoToCount := SpecialReport{UUID: newSpecialReportUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: newAlternativeIds}

	assert.NoError(t, specialreportsDriver.Write(specialreportTwoToCount), "Failed to write specialreport")

	nr, err = specialreportsDriver.Count()
	assert.Equal(t, 2, nr, "Should be 2 specialreports in DB - count differs")
	assert.NoError(t, err, "An unexpected error occurred during count")

	cleanUp(t, specialreportUUID, specialreportsDriver)
	cleanUp(t, newSpecialReportUUID, specialreportsDriver)
}

func readSpecialReportForUUIDAndCheckFieldsMatch(t *testing.T, specialreportsDriver service, uuid string, expectedSpecialReport SpecialReport) {

	storedSpecialReport, found, err := specialreportsDriver.Read(uuid)

	assert.NoError(t, err, "Error finding specialreport for uuid %s", uuid)
	assert.True(t, found, "Didn't find specialreport for uuid %s", uuid)
	assert.Equal(t, expectedSpecialReport, storedSpecialReport, "specialreports should be the same")
}

func getSpecialReportsCypherDriver(t *testing.T) service {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(url, conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	service := NewCypherSpecialReportsService(db)
	service.Initialise()
	return service
}

func cleanUp(t *testing.T, uuid string, specialreportsDriver service) {
	found, err := specialreportsDriver.Delete(uuid)
	assert.True(t, found, "Didn't manage to delete specialreport for uuid %", uuid)
	assert.NoError(t, err, "Error deleting specialreport for uuid %s", uuid)
}
