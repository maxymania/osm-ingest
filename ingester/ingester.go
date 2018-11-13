/*
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <http://unlicense.org/>
*/



package ingester

import "github.com/paulmach/orb"
import "github.com/paulmach/orb/encoding/wkb"
import "encoding/binary"
import "encoding/hex"

import "github.com/maxymania/osm-ingest/projection"
import "database/sql"
import "github.com/paulmach/osm"

import "github.com/lib/pq"
import "github.com/lib/pq/hstore"
import "log"

var mymerc = projection.WGS84Mercator

var lE = binary.LittleEndian

func encode(g orb.Geometry) ([]byte,error){
	b,e := wkb.Marshal(g,lE)
	if e!=nil { return nil,e }
	b2 := make([]byte,1+4+4,len(b)+4)
	b2[0]=b[0]
	lE.PutUint32(b2[1:],lE.Uint32(b[1:])|0x20000000)
	lE.PutUint32(b2[5:],uint32(mymerc.SRID()))
	b2 = append(b2,b[5:]...)
	return b2,nil
}
func encodeHex(g orb.Geometry) (string,error){
	b,e := encode(g)
	return hex.EncodeToString(b),e
}

type Ingester struct{
	db *sql.DB
	
	nodet *sql.Tx
	nodes *sql.Stmt
	
}
func (i *Ingester) Init(db *sql.DB) error {
	i.db = db
	_,err := db.Exec(`
	CREATE TABLE IF NOT EXISTS onodes(
		"fid" bigint,
		"ver" integer,
		"tags" hstore,
		"way" geometry
	)
	`)
	if err!=nil { return err }
	db.Exec(`CREATE INDEX onodes_pk ON onodes(fid)`)
	db.Exec(`CREATE INDEX onodes_geom ON onodes USING gist (way)`)
	
	return nil
}
func (i *Ingester) commitNodes() {
	if i.nodes!=nil {
		i.nodes.Close()
		i.nodes = nil
	}
	if i.nodet!=nil {
		err := i.nodet.Commit()
		if err!=nil { i.nodet.Rollback() }
	}
}
func (i *Ingester) Finish() {
	commitNodes()
}
func (i *Ingester) Add(n osm.Object) error {
	var err error
	switch v := n.(type) {
	case *osm.Node:
		if i.nodes==nil {
			i.nodet,err = i.db.Begin()
			if err!=nil { return err }
			pq.CopyIn("onodes", "fid", "ver", "tags", "way")
			i.nodes,err = i.nodet.Prepare(`INSERT INTO onodes (fid,ver,tags,way) VALUES ($1,$2,$3,$4)`) // pq.CopyIn("onodes", "fid", "ver", "tags", "way")
			if err!=nil { return err }
			//log.Printf("%q %v",pq.CopyIn("onodes", "fid", "ver", "tags", "way"),err)
		}
		return i.addNode(v)
	case *ocm.Way:
		return i.addWay(v)
	}
	return nil
}
func (i *Ingester) addNode(n *osm.Node) error {
	pt := mymerc.Point(n.Point())
	
	tags := hstore.Hstore{make(map[string]sql.NullString)}
	for _,tag := range n.Tags {
		if osm.UninterestingTags[tag.Key] { continue }
		tags.Map[tag.Key] = sql.NullString{tag.Value,true}
	}
	
	var vtags interface{}
	if len(tags.Map)>0 { vtags = tags } else { tags.Map = nil; vtags = tags }
	
	x,e := encodeHex(pt)
	if e!=nil { return e }
	_,e = i.nodes.Exec(int64(n.FeatureID()),int64(n.Version),vtags,x)
	
	if e!=nil {
		log.Printf("(%d %d %v %vx)",int64(n.FeatureID()),int64(n.Version),vtags,x)
		return e
	}
	return nil
}
func (i *Ingester) addWay(n *osm.Node) error {
	
}

