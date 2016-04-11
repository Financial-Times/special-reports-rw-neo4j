package specialreports

import (
	"os"
	"testing"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

var specialReportsDriver baseftrwapp.Service

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"

	specialReportsDriver = getSpecialReportsCypherDriver(t)

	specialReportToDelete := SpecialReport{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(specialReportsDriver.Write(specialReportToDelete), "Failed to write special report")

	found, err := specialReportsDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete special report for uuid %", uuid)
	assert.NoError(err, "Error deleting special report for uuid %s", uuid)

	p, found, err := specialReportsDriver.Read(uuid)

	assert.Equal(SpecialReport{}, p, "Found special report %s who should have been deleted", p)
	assert.False(found, "Found special report for uuid %s who should have been deleted", uuid)
	assert.NoError(err, "Error trying to find special report for uuid %s", uuid)
}

func TestCreateAllValuesPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	specialReportsDriver = getSpecialReportsCypherDriver(t)

	specialReportToWrite := SpecialReport{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(specialReportsDriver.Write(specialReportToWrite), "Failed to write special report")

	readSpecialReportForUUIDAndCheckFieldsMatch(t, uuid, specialReportToWrite)

	cleanUp(t, uuid)
}

func TestCreateHandlesSpecialCharacters(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	specialReportsDriver = getSpecialReportsCypherDriver(t)

	specialReportToWrite := SpecialReport{UUID: uuid, CanonicalName: "Test 'special chars", TmeIdentifier: "TME_ID"}

	assert.NoError(specialReportsDriver.Write(specialReportToWrite), "Failed to write special report")

	readSpecialReportForUUIDAndCheckFieldsMatch(t, uuid, specialReportToWrite)

	cleanUp(t, uuid)
}

func TestCreateNotAllValuesPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	specialReportsDriver = getSpecialReportsCypherDriver(t)

	specialReportToWrite := SpecialReport{UUID: uuid, CanonicalName: "Test"}

	assert.NoError(specialReportsDriver.Write(specialReportToWrite), "Failed to write special report")

	readSpecialReportForUUIDAndCheckFieldsMatch(t, uuid, specialReportToWrite)

	cleanUp(t, uuid)
}

func TestUpdateWillRemovePropertiesNoLongerPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	specialReportsDriver = getSpecialReportsCypherDriver(t)

	specialReportToWrite := SpecialReport{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(specialReportsDriver.Write(specialReportToWrite), "Failed to write special report")
	readSpecialReportForUUIDAndCheckFieldsMatch(t, uuid, specialReportToWrite)

	updatedSpecialReport := SpecialReport{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(specialReportsDriver.Write(updatedSpecialReport), "Failed to write updated special report")
	readSpecialReportForUUIDAndCheckFieldsMatch(t, uuid, updatedSpecialReport)

	cleanUp(t, uuid)
}

func TestConnectivityCheck(t *testing.T) {
	assert := assert.New(t)
	specialReportsDriver = getSpecialReportsCypherDriver(t)
	err := specialReportsDriver.Check()
	assert.NoError(err, "Unexpected error on connectivity check")
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

func readSpecialReportForUUIDAndCheckFieldsMatch(t *testing.T, uuid string, expectedSpecialReport SpecialReport) {
	assert := assert.New(t)
	storedSpecialReport, found, err := specialReportsDriver.Read(uuid)

	assert.NoError(err, "Error finding special report for uuid %s", uuid)
	assert.True(found, "Didn't find special report for uuid %s", uuid)
	assert.Equal(expectedSpecialReport, storedSpecialReport, "special reports should be the same")
}

func TestWritePrefLabelIsAlsoWrittenAndIsEqualToName(t *testing.T) {
	assert := assert.New(t)
	specialReportsDriver := getSpecialReportsCypherDriver(t)
	uuid := "12345"
	specialReportToWrite := SpecialReport{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(specialReportsDriver.Write(specialReportToWrite), "Failed to write special report")

	result := []struct {
		PrefLabel string `json:"t.prefLabel"`
	}{}

	getPrefLabelQuery := &neoism.CypherQuery{
		Statement: `
				MATCH (t:SpecialReport {uuid:"12345"}) RETURN t.prefLabel
				`,
		Result: &result,
	}

	err := specialReportsDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{getPrefLabelQuery})
	assert.NoError(err)
	assert.Equal("Test", result[0].PrefLabel, "PrefLabel should be 'Test")
	cleanUp(t, uuid)
}

func cleanUp(t *testing.T, uuid string) {
	assert := assert.New(t)
	found, err := specialReportsDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete special report for uuid %", uuid)
	assert.NoError(err, "Error deleting special report for uuid %s", uuid)
}
