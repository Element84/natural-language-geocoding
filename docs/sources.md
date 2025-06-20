# Place Sources

Natural Language Geocoding (NLG) requires a source of places so that when a description of a spatial area is geocoded it can find the geometry of any named places. For example, if geocoding "within 3 miles of Annapolis, Maryland", NLG will need to retrieve a geometry representing the area of "Annapolis, Maryland".

The source for finding places is configured through environment variables.

## OpenSearch (aka Geocode Index)

OpenSearch can be used as a geocoding database and is the recommended approach as it provides the best relevant places for place names. Throughout the codebase this is referred to as the Geocode Index.

When configuring an OpenSearch cluster in AWS an r7g.large.search instance type with at least 2 nodes is recommended.

### Configuration

When calling `extract_geometry_from_text` a `PlaceLookup` implementation is required. The `GeocodeIndexPlaceLookup` class implements `PlaceLookup` by searching OpenSearch for the most relevant place.

**Environment Variables**

* `GEOCODE_INDEX_HOST` - The host name of the OpenSearch index.
* `GEOCODE_INDEX_PORT` - The port to connect to the OpenSearch index.
* `GEOCODE_INDEX_REGION` - The AWS region where the OpenSearch cluster is hosted.
* `GEOCODE_INDEX_USERNAME` - The username of the user to connect to OpenSearch.
* `GEOCODE_INDEX_PASSWORD` - The password of the user to connect to OpenSearch.

### Populating OpenSearch

OpenSearch must be populated with places that can be found. This is done by pulling places from external sources, mapping the fields into this libraries place representation, and then ingesting into OpenSearch.

Run these scripts in **this order** to populate OpenSearch. The order is important as Who's On First data is used to identify the hierarchies (parent regions) of places as they are ingested. Note that this will take several hours as there are millions of places being downloaded and saved. It's best to run these scripts on an instance in AWS in the same region as the OpenSearch cluster.

1. `scripts/ingest_wof_places.sh` - Ingests places from Who's On First.
2. `scripts/ingest_ne_places.sh` - Ingests places from Natural Earth.
3. `scripts/ingest_composed_places.sh` - Ingests "composed" places which are built from combinations of other places. For example, West Africa is a place composed of various countries on the west coast of Africa.

### Data Sources and Licenses

* [Who's On First](https://whosonfirst.org/) data is used which is a collection of original work and other existing data sources. See the [license page](https://whosonfirst.org/docs/licenses/).
* [Natural Earth](https://www.naturalearthdata.com/) is in the public domain. See https://www.naturalearthdata.com/about/terms-of-use/


## OpenStreetMap

**Deprecated:** Use of OpenStreetMap is deprecated.

OpenStreetMap can be used as a source of places which doesn't require any extra databases.  Users of this library must follow OpenStreetMap's [attribution guidelines](https://osmfoundation.org/wiki/Licence/Attribution_Guidelines)

Users must also conform to the [Nominatim Usage Policy](https://operations.osmfoundation.org/policies/nominatim/). The environment variable `NOMINATIM_USER_AGENT` should be set to identify your application.
