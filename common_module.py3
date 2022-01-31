
import time
import os
import datetime
import pytz

from google.cloud import datacatalog_v1, bigquery
from google.protobuf.timestamp_pb2 import Timestamp
from google.api_core.exceptions import NotFound,PermissionDenied,Forbidden

# Declare variables
now       = time.time()
seconds   = int(now)
nanos     = int((now - seconds) * 10**9)
timestamp = Timestamp(seconds=seconds, nanos=nanos)

entry_group_id      = os.environ.get('ENTRY_GROUP')
project_id          = os.environ.get('GCP_PROJECT')
location            = os.environ.get('FUNCTION_REGION')

datacatalog         = datacatalog_v1.DataCatalogClient()
datacatalog_types   = datacatalog_v1.types
resource_name       = 'projects/{}/locations/{}'.format(project_id, location)

# Accumulate all currently existing Technical metadata entries in DC
def dc_get_entries_by_eg_tech():

    """Extracts Data Catalog data assets for a given project_id."""
    datacatalog = datacatalog_v1.DataCatalogClient()
    scope = datacatalog_v1.types.SearchCatalogRequest.Scope()
    scope.include_project_ids.append(project_id)
    pageSize = 1000

    entry = datacatalog.search_catalog(
      request={'scope': scope, 'page_size': pageSize})

    bq_ds_list = []
    bq_table_list = []
    for result in entry.results:

      entryGroupElems = result.relative_resource_name.split("/")
      if "entryGroups" in entryGroupElems:
        if result.linked_resource.find(project_id) != -1:
            bq_asset = result.linked_resource.split("/{}".format(project_id))[1]
            if "tables/" in bq_asset:
                bq_table_list.insert(0,bq_asset)
        else:
            bq_ds_list.insert(0,result.linked_resource)

    return bq_ds_list, bq_table_list

# Accumulate all attributes of a tag template
def list_attr_of_tag_template(tt):

    response            = None
    attributes          = []
    attributeTypes      = []
    attributesRequired  = []
    name = 'projects/{}/locations/{}/tagTemplates/{}'.format(project_id,location,tt)
    try:
        response = datacatalog.get_tag_template(name=name)
        for field in response.fields:
            if 'is_required' in response.fields[field]:
                attributesRequired.append(field)
            attributes.append(field)
            attributeTypes.append(str(response.fields[field].type_.primitive_type).replace('PrimitiveType.',''))
    except (NotFound, PermissionDenied, Forbidden) as e:
        pass

    return attributes, attributeTypes, attributesRequired

# Generate a Tag metadata - table
def generate_tag_table(tt,tt_attributes,tt_attributesTypes,tt_attributesRequired,row):

    tag = datacatalog_types.Tag()
    tag.template = "projects/{}/locations/{}/tagTemplates/{}".format(project_id,location,tt)
    for i in range(len(tt_attributes)):

        # If the column is a required field in the tag template then use
        # default value depending on the field datatype
        if (tt_attributes[i] in row and 
            tt_attributes[i] in tt_attributesRequired and
            isNaN(row[tt_attributes[i]])):

            tag.fields[tt_attributes[i]] = datacatalog_types.TagField()
            if tt_attributesTypes[i] == 'STRING':
                tag.fields[tt_attributes[i]].string_value    = ' '
            elif tt_attributesTypes[i] == 'DOUBLE':
                tag.fields[tt_attributes[i]].double_value    = 0
            elif tt_attributesTypes[i] == 'BOOL':
                tag.fields[tt_attributes[i]].bool_value      = False
            elif tt_attributesTypes[i] == 'DATETIME':
                tag.fields[tt_attributes[i]].timestamp_value = ' '
            else:
                tag.fields[tt_attributes[i]].string_value = row[tt_attributes[i]] 

            continue
        
        if (tt_attributes[i] in row and isNaN(row[tt_attributes[i]]) == False):

            tag.fields[tt_attributes[i]] = datacatalog_types.TagField()
            if tt_attributesTypes[i] == 'STRING':
                tag.fields[tt_attributes[i]].string_value     = str(row[tt_attributes[i]])
            elif tt_attributesTypes[i] == 'DOUBLE':
                tag.fields[tt_attributes[i]].double_value     = float(row[tt_attributes[i]].replace(',', ''))
            elif tt_attributesTypes[i] == 'BOOL':
                tag.fields[tt_attributes[i]].bool_value       = True if row[tt_attributes[i]]=="TRUE" else False
            elif tt_attributesTypes[i] == 'DATETIME':
                tag.fields[tt_attributes[i]].timestamp_value  = row[tt_attributes[i]]
            else:
                tag.fields[tt_attributes[i]].string_value     = row[tt_attributes[i]]
        
    return tag

# Insert audit trail in BQ table
def bq_insert_audtrl():

    bq_client       = bigquery.Client()
    audtrl_now 	    = pytz.utc.localize(datetime.datetime.now())
    audtrl_est_now  = audtrl_now.astimezone(pytz.timezone("America/New_York"))

    table_id = 'ati-gblbi-c3-poc.data_catalog_extracts.batch_extracts_audtrl'
    rows_to_insert = [
        {
            u"step_name": u"DC - Data Lineage Table Level", 
            u"step_description": u"Data Catalog - Data Lineage Table Level tags attached",
            u"inserted": str(audtrl_est_now)
        }
    ]

    errors = bq_client.insert_rows_json(table_id, rows_to_insert)   
    if errors == []:
        print("Row inserted to BQ audit trail table.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

    return

def isNaN(string):
    return string != string