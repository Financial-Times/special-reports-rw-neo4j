package specialreports

import (
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"sort"
)

const (
	specialreportUUID    = "12345"
	newSpecialReportUUID = "123456"
	tmeID                = "TME_ID"
	newTmeID             = "NEW_TME_ID"
	prefLabel            = "Test"
	specialCharPrefLabel = "Test 'special chars"
)

func specialreportToWrite(uuid string, prefLabel string, tmeIds [] string, uuids []string) SpecialReport {
	sort.Strings(tmeIds)
	sort.Strings(uuids)
	alternativeIdentifiers := alternativeIdentifiers{TME: tmeIds, UUIDS: uuids}
	sr := SpecialReport{UUID: uuid, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}
	return sr
}

func defaultTypes() []string {
	var defaultTypes = []string{"Thing", "Concept", "Classification", "SpecialReport"}
	sort.Strings(defaultTypes)
	return defaultTypes
}

func TestConnectivityCheck(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	err := specialreportsDriver.Check()
	assert.NoError(t, err, "Unexpected error on connectivity check")
}

func TestPrefLabelIsCorrectlyWritten(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	defer cleanUp(t, specialreportUUID, specialreportsDriver)

	sr := specialreportToWrite(specialreportUUID, prefLabel, nil, []string{specialreportUUID})
	err := specialreportsDriver.Write(sr)
	assert.NoError(t, err, "ERROR happened during write time")

	storedSpecialReport, found, err := specialreportsDriver.Read(specialreportUUID)
	assert.NoError(t, err, "ERROR happened during read time")
	assert.True(t, found, "Failed to read a Special Report that we have written.")
	assert.NotEmpty(t, storedSpecialReport)
	assert.Equal(t, prefLabel, storedSpecialReport.(SpecialReport).PrefLabel, "PrefLabel should be %s", prefLabel)
}

func TestPrefLabelSpecialCharactersAreHandledByCreate(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	defer cleanUp(t, specialreportUUID, specialreportsDriver)

	specialreportToWrite := specialreportToWrite(specialreportUUID, specialCharPrefLabel, []string{}, []string{specialreportUUID})

	assert.NoError(t, specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")
	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes()
	//check if specialreportToWrite is the same with the one inside the DB
	readSpecialReportForUUIDAndCheckFieldsMatch(t, specialreportsDriver, specialreportUUID, specialreportToWrite)
}

func TestCreateCompleteSpecialReportWithPropsAndIdentifiers(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	defer cleanUp(t, specialreportUUID, specialreportsDriver)

	specialreportToWrite := specialreportToWrite(specialreportUUID, prefLabel, []string{tmeID}, []string{specialreportUUID})

	assert.NoError(t, specialreportsDriver.Write(specialreportToWrite), "Failed to write specialreport")

	//add default types that will be automatically added by the writer
	specialreportToWrite.Types = defaultTypes()

	//check if specialreportToWrite is the same with the one inside the DB
	readSpecialReportForUUIDAndCheckFieldsMatch(t, specialreportsDriver, specialreportUUID, specialreportToWrite)

}

func TestUpdateWillRemovePropertiesAndIdentifiersNoLongerPresent(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	defer cleanUp(t, specialreportUUID, specialreportsDriver)

	assert.NoError(t, specialreportsDriver.Write(specialreportToWrite(specialreportUUID, prefLabel, []string{}, []string{specialreportUUID})), "Failed to write specialreport")

	updatedSpecialReport := specialreportToWrite(specialreportUUID, specialCharPrefLabel, []string{}, []string{specialreportUUID})
	assert.NoError(t, specialreportsDriver.Write(updatedSpecialReport), "Failed to write updated specialreport")
	//add default types that will be automatically added by the writer
	updatedSpecialReport.Types = defaultTypes()
	readSpecialReportForUUIDAndCheckFieldsMatch(t, specialreportsDriver, specialreportUUID, updatedSpecialReport)
}

func TestDelete(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)

	specialreportToDelete :=  specialreportToWrite(specialreportUUID, prefLabel, []string{tmeID}, []string{specialreportUUID} )
	assert.NoError(t, specialreportsDriver.Write(specialreportToDelete), "Failed to write specialreport")

	found, err := specialreportsDriver.Delete(specialreportUUID)
	assert.NoError(t, err, "Error deleting specialreport for uuid %s", specialreportUUID)
	assert.True(t, found, "Didn't delete specialreport for uuid %", specialreportUUID)

	p, found, err := specialreportsDriver.Read(specialreportUUID)

	assert.NoError(t, err, "Error trying to find specialreport for uuid %s", specialreportUUID)
	assert.False(t, found, "Found specialreport for uuid %s who should have been deleted", specialreportUUID)
	assert.Equal(t, SpecialReport{}, p, "Found specialreport %s who should have been deleted", p)
}

func TestCount(t *testing.T) {
	specialreportsDriver := getSpecialReportsCypherDriver(t)
	defer cleanUp(t, specialreportUUID, specialreportsDriver)
	defer cleanUp(t, newSpecialReportUUID, specialreportsDriver)

	specialreportOneToCount := specialreportToWrite(specialreportUUID, prefLabel, []string{tmeID},[]string{specialreportUUID})
	specialreportTwoToCount := specialreportToWrite(newSpecialReportUUID, specialCharPrefLabel, []string{newTmeID},[]string{newSpecialReportUUID})

	assert.NoError(t, specialreportsDriver.Write(specialreportOneToCount), "Failed to write specialreport")
	nr, err := specialreportsDriver.Count()

	assert.NoError(t, err, "An unexpected error occurred during count")
	assert.Equal(t, 1, nr, "Should be 1 specialreports in DB - count differs")

	assert.NoError(t, specialreportsDriver.Write(specialreportTwoToCount), "Failed to write specialreport")
	nr, err = specialreportsDriver.Count()

	assert.NoError(t, err, "An unexpected error occurred during count")
	assert.Equal(t, 2, nr, "Should be 2 specialreports in DB - count differs")
}

func readSpecialReportForUUIDAndCheckFieldsMatch(t *testing.T, specialreportsDriver service, uuid string, expectedSpecialReport SpecialReport) {

	storedSpecialReport, found, err := specialreportsDriver.Read(uuid)

	assert.NoError(t, err, "Error finding specialreport for uuid %s", uuid)
	assert.True(t, found, "Didn't find specialreport for uuid %s", uuid)

	actualSpecialReport := storedSpecialReport.(SpecialReport)
	sort.Strings(actualSpecialReport.AlternativeIdentifiers.TME)
	sort.Strings(actualSpecialReport.AlternativeIdentifiers.UUIDS)
	sort.Strings(actualSpecialReport.Types)
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
