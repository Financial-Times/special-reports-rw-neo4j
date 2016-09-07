package specialreports

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

type service struct {
	conn neoutils.NeoConnection
}

// NewCypherSpecialReportsService provides functions for create, update, delete operations on special reports in Neo4j,
// plus other utility functions needed for a service
func NewCypherSpecialReportsService(neoConnection neoutils.NeoConnection) service {
	return service{neoConnection}
}

func (s service) Initialise() error {
	return s.conn.EnsureConstraints(map[string]string{
		"Thing":          "uuid",
		"Concept":        "uuid",
		"Classification": "uuid",
		"SpecialReport":  "uuid",
		"TMEIdentifier":  "value",
		"UPPIdentifier":  "value"})
}

func (s service) Read(uuid string) (interface{}, bool, error) {
	results := []SpecialReport{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:SpecialReport {uuid:{uuid}})
OPTIONAL MATCH (upp:UPPIdentifier)-[:IDENTIFIES]->(n)
OPTIONAL MATCH (tme:TMEIdentifier)-[:IDENTIFIES]->(n)
return distinct n.uuid as uuid, n.prefLabel as prefLabel, labels(n) as types, {uuids:collect(distinct upp.value), TME:collect(distinct tme.value)} as alternativeIdentifiers`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return SpecialReport{}, false, err
	}

	if len(results) == 0 {
		return SpecialReport{}, false, nil
	}

	return results[0], true, nil
}

func (s service) Write(thing interface{}) error {

	specialReport := thing.(SpecialReport)

	//cleanUP all the previous IDENTIFIERS referring to that uuid
	deletePreviousIdentifiersQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {uuid:{uuid}})
		OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i)
		DELETE iden, i`,
		Parameters: map[string]interface{}{
			"uuid": specialReport.UUID,
		},
	}

	//create-update node for SpecialReport
	createSpecialReportQuery := &neoism.CypherQuery{
		Statement: `MERGE (n:Thing {uuid: {uuid}})
					set n={allprops}
					set n :Concept
					set n :Classification
					set n :SpecialReport
		`,
		Parameters: map[string]interface{}{
			"uuid": specialReport.UUID,
			"allprops": map[string]interface{}{
				"uuid":      specialReport.UUID,
				"prefLabel": specialReport.PrefLabel,
			},
		},
	}

	queryBatch := []*neoism.CypherQuery{deletePreviousIdentifiersQuery, createSpecialReportQuery}

	//ADD all the IDENTIFIER nodes and IDENTIFIES relationships
	for _, alternativeUUID := range specialReport.AlternativeIdentifiers.TME {
		alternativeIdentifierQuery := createNewIdentifierQuery(specialReport.UUID, tmeIdentifierLabel, alternativeUUID)
		queryBatch = append(queryBatch, alternativeIdentifierQuery)
	}

	for _, alternativeUUID := range specialReport.AlternativeIdentifiers.UUIDS {
		alternativeIdentifierQuery := createNewIdentifierQuery(specialReport.UUID, uppIdentifierLabel, alternativeUUID)
		queryBatch = append(queryBatch, alternativeIdentifierQuery)
	}
	return s.conn.CypherBatch(queryBatch)
}

func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					CREATE (i:Identifier {value:{value}})
					MERGE (t)<-[:IDENTIFIES]-(i)
					set i : %s `, identifierLabel)
	query := &neoism.CypherQuery{
		Statement: statementTemplate,
		Parameters: map[string]interface{}{
			"uuid":  uuid,
			"value": identifierValue,
		},
	}
	return query
}

func (s service) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (t:Thing {uuid: {uuid}})
			OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i:Identifier)
			REMOVE t:Concept
			REMOVE t:Classification
			REMOVE t:SpecialReport
			DELETE iden, i
			SET t={uuid:{uuid}}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}

	removeNodeIfUnused := &neoism.CypherQuery{
		Statement: `
			MATCH (t:Thing {uuid: {uuid}})
			OPTIONAL MATCH (t)-[a]-(x)
			WITH t, count(a) AS relCount
			WHERE relCount = 0
			DELETE t
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{clearNode, removeNodeIfUnused})

	s1, err := clearNode.Stats()
	if err != nil {
		return false, err
	}

	var deleted bool
	if s1.ContainsUpdates && s1.LabelsRemoved > 0 {
		deleted = true
	}

	return deleted, err
}

func (s service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := SpecialReport{}
	err := dec.Decode(&sub)
	return sub, sub.UUID, err
}

func (s service) Check() error {
	return neoutils.Check(s.conn)
}

func (s service) Count() (int, error) {

	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:SpecialReport) return count(n) as c`,
		Result:    &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}
