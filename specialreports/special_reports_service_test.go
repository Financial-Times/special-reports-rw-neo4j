package specialreports

import (
	"os"
	"testing"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
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
	assert := assert.New(t)
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	err := specialreportsDriver.Check()
	assert.NoError(err, "Unexpected error on connectivity check")
}

func TestPrefLabelIsCorrectlyWritten(t *testing.T) {
	assert := assert.New(t)
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	err := specialreportsDriver.Write(specialreportToWrite)
	assert.NoError(err, "ERROR happened during write time")

	storedSpecialReport, found, err := specialreportsDriver.Read(specialreportUUID)
	assert.NoError(err, "ERROR happened during read time")
	assert.Equal(true, found)
	assert.NotEmpty(storedSpecialReport)

	assert.Equal(prefLabel, storedSpecialReport.(SpecialReport).PrefLabel, "PrefLabel should be "+prefLabel)
	cleanUp(assert, specialreportUUID, specialreportsDriver)
}

func TestPrefLabelSpecialCharactersAreHandledByCreate(t *testing.T) {
	assert := assert.New(t)
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")

	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes
	//check if specialreportToWrite is the same with the one inside the DB
	readSpecialReportForUUIDAndCheckFieldsMatch(assert, specialreportsDriver, specialreportUUID, specialreportToWrite)
	cleanUp(assert, specialreportUUID, specialreportsDriver)
}

func TestCreateCompleteSpecialReportWithPropsAndIdentifiers(t *testing.T) {
	assert := assert.New(t)
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")

	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes
	//check if specialreportToWrite is the same with the one inside the DB
	readSpecialReportForUUIDAndCheckFieldsMatch(assert, specialreportsDriver, specialreportUUID, specialreportToWrite)
	cleanUp(assert, specialreportUUID, specialreportsDriver)
}

func TestUpdateWillRemovePropertiesAndIdentifiersNoLongerPresent(t *testing.T) {
	assert := assert.New(t)
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	allAlternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{specialreportUUID}}
	specialreportToWrite := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: allAlternativeIdentifiers}

	assert.NoError(specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")
	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes
	readSpecialReportForUUIDAndCheckFieldsMatch(assert, specialreportsDriver, specialreportUUID, specialreportToWrite)

	tmeAlternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	updatedSpecialReport := SpecialReport{UUID: specialreportUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: tmeAlternativeIdentifiers}

	assert.NoError(specialreportsDriver.Write(updatedSpecialReport), "Failed to write updated specialreport")
	//add default types that will be automatically added by the writer
	updatedSpecialReport.Types = defaultTypes
	readSpecialReportForUUIDAndCheckFieldsMatch(assert, specialreportsDriver, specialreportUUID, updatedSpecialReport)

	cleanUp(assert, specialreportUUID, specialreportsDriver)
}

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	specialreportToDelete := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(specialreportsDriver.Write(specialreportToDelete), "Failed to write specialreport")

	found, err := specialreportsDriver.Delete(specialreportUUID)
	assert.True(found, "Didn't manage to delete specialreport for uuid %", specialreportUUID)
	assert.NoError(err, "Error deleting specialreport for uuid %s", specialreportUUID)

	p, found, err := specialreportsDriver.Read(specialreportUUID)

	assert.Equal(SpecialReport{}, p, "Found specialreport %s who should have been deleted", p)
	assert.False(found, "Found specialreport for uuid %s who should have been deleted", specialreportUUID)
	assert.NoError(err, "Error trying to find specialreport for uuid %s", specialreportUUID)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	alternativeIds := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{specialreportUUID}}
	specialreportOneToCount := SpecialReport{UUID: specialreportUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIds}

	assert.NoError(specialreportsDriver.Write(specialreportOneToCount), "Failed to write specialreport")

	nr, err := specialreportsDriver.Count()
	assert.Equal(1, nr, "Should be 1 specialreports in DB - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	newAlternativeIds := alternativeIdentifiers{TME: []string{newTmeID}, UUIDS: []string{newSpecialReportUUID}}
	specialreportTwoToCount := SpecialReport{UUID: newSpecialReportUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: newAlternativeIds}

	assert.NoError(specialreportsDriver.Write(specialreportTwoToCount), "Failed to write specialreport")

	nr, err = specialreportsDriver.Count()
	assert.Equal(2, nr, "Should be 2 specialreports in DB - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	cleanUp(assert, specialreportUUID, specialreportsDriver)
	cleanUp(assert, newSpecialReportUUID, specialreportsDriver)
}

func readSpecialReportForUUIDAndCheckFieldsMatch(assert *assert.Assertions, specialreportsDriver service, uuid string, expectedSpecialReport SpecialReport) {

	storedSpecialReport, found, err := specialreportsDriver.Read(uuid)

	assert.NoError(err, "Error finding specialreport for uuid %s", uuid)
	assert.True(found, "Didn't find specialreport for uuid %s", uuid)
	assert.Equal(expectedSpecialReport, storedSpecialReport, "specialreports should be the same")
}

func getSpecialReportsCypherDriver(t *testing.T) service {
	assert := assert.New(t)
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	db, err := neoism.Connect(url)
	assert.NoError(err, "Failed to connect to Neo4j")
	return NewCypherSpecialReportsService(neoutils.StringerDb{db}, db)
}

func cleanUp(assert *assert.Assertions, uuid string, specialreportsDriver service) {
	found, err := specialreportsDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete specialreport for uuid %", uuid)
	assert.NoError(err, "Error deleting specialreport for uuid %s", uuid)
}
