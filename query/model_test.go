package query

import (
	"testing"
	"time"
)

type SampleField struct {
	ID         int       `src:"sf" column:"id"`
	Geom       string    `src:"sf" column:"geom" dbread:"st_asgeojson($column)" dbwrite:"st_fromgeojson('$value')" read:"geoJsonRead" write:"geoJsonWrite"`
	SampleDate time.Time `src:"sf" column:"sampledate"`
	FarmId     int       `src:"sf" column:"farmid" alias:"FarmId"`
}

func (ctx *SampleField) GetSources() ([]string, []string, []string) {
	return []string{
			"from",
		}, []string{
			"schema.sample_field",
		}, []string{
			"sf",
		}
}

func TestGetModelInfo(t *testing.T) {
	info := GetModelInfo(&SampleField{}, FieldName)
	if info["ID"].ColumnName != "id" ||
		len(info["ID"].WriteDatabaseConverter) > 0 {
		t.Errorf("invalid info for ID Property")
		return
	}
	if info["Geom"].ColumnName != "st_asgeojson(geom)" ||
		info["Geom"].WriteDatabaseConverter != "st_fromgeojson('$value')" {
		t.Errorf("invalid info for Geom Property")
		return
	}
	if info["SampleDate"].ColumnName != "sampledate" ||
		len(info["SampleDate"].WriteDatabaseConverter) > 0 {
		t.Errorf("invalid info for SampleDate Property")
		return
	}
	if info["FarmId"].ColumnName != "farmid" ||
		len(info["FarmId"].WriteDatabaseConverter) > 0 ||
		info["FarmId"].Alias != "FarmId" {
		t.Errorf("invalid info for FarmId Property")
		return
	}
}
