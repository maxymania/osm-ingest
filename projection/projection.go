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

package projection

import "github.com/paulmach/orb"
import "math"

type IProjection interface{
	SRID() int
	Point(r orb.Point) orb.Point
}

func identity(r orb.Point) orb.Point { return r }

const mercRadius = 6378137.0
const fromDegree = math.Pi/180.0
const fromDegree2 = math.Pi/360.0
const toDegree = 180.0/math.Pi
const piFour = math.Pi / 4

func mercate(p orb.Point) orb.Point {
	p[0] = mercRadius * fromDegree * p[0]
	p[1] = mercRadius * math.Log(math.Tan(piFour+(p[1]*fromDegree2)))
	return p
}


const mercMajor = 6378137.0
const mercMinor = 6356752.3142
const mercMinMaj = mercMinor/mercMajor
const mercEs = 1.0-(mercMinMaj*mercMinMaj)

func wgs84(p orb.Point) orb.Point {
	p[0] = mercMajor * fromDegree * p[0]
	
	/*
	if (lat > 89.5) {
		lat = 89.5;
	}
	if (lat < -89.5) {
		lat = -89.5;
	}
	*/
	lat := math.Min(89.5,math.Max(-89.5,p[1]))
	
	eccent := math.Sqrt(mercEs)
	phi := lat * fromDegree
	sinphi := math.Sin(phi)
	con := eccent * sinphi
	com := 0.5 * eccent
	con = math.Pow( ( (1-con)/(1+con) ) , com)
	ts := math.Tan(0.5 * ((math.Pi*0.5)-phi))/con
	y := 0-mercMajor * math.Log(ts)
	
	p[1] = y
	
	return p
}


type Projection uint
const (
	LatLon Projection = iota
	PseudoMercator
	WGS84Mercator
	maxProjection
	
	WebMercator = PseudoMercator
)

// http://www.volkerschatz.com/net/osm/osm2pgsql-usage.html
var srids = [maxProjection]int {
	4326,
	900913, /* 3857 (900913, 3785) */
	3395,
}

func (p Projection) SRID() int { return srids[p] }

var convs = [maxProjection]func(r orb.Point) orb.Point {
	identity,
	mercate,
	wgs84,
}

func (p Projection) Point(r orb.Point) orb.Point { return convs[p](r) }

