## project
{
  "last_release_date": null, 
  "type": "program", 
  "summary_description": null, 
  "dataset_url": null, 
  "version": null, 
  "*release_name": null, 
  "*name": null, 
  "*dbgap_accession_number": null
}


## fileset
{
    "last_release_date": null, 
    "*projects": {
      "id": fetched_project_id
    }, 
    "*description": fileset_description, 
    "*fileset_name": fileset_name, 
    "version": null, 
    "*type": "fileset", 
    "*submitter_id": fileset_submitter_id
  }

  ## ILDF
  ##in DSS type=cram, index, etc., on datastage that is format
  file_format = file["type"]
  ##in datastage this is WGS, WES, etc., which is sample.assay in dss data
  file_type = file["sample"]["assay"]
  file_path = file["path"]
  file_name = file["name"]
  file_size = file["size"]
  sample_id = file["sample_id"]
  cmc_submitter_id = project_name+"_core_metadata_collection"
  ## currently missing ref_build and data_category(genotype, expression, etc.) because not in DSS data
  idlf_obj = {
    "*data_type": file_type, 
    "filesets": {
      "submitter_id": fileset_submitter_id
    }, 
    "*consent": c, 
    "core_metadata_collections": {
      "submitter_id": cmc_submitter_id
    }, 
    "*type": "individual_level_data_file", 
    "*file_path": file_path, 
    "*data_format": file_format, 
    "*file_name": file_name, 
    "*md5sum": null, 
    "*file_size": file_size, 
    "*samples": {
      "submitter_id": sample_id
    }, 
    "*submitter_id": null
  }

  ## ALDF
  file_type = ???? not in data (WGS WES etc.)
  file_size = file["size"]
  file_path = file["path"]
  file_name = file["name"]
  file_format = file["type"]
  cmc_submitter_id = program_name+"_core_metadata_collection"
  file_submitter_id = file_name + "_" + file_format + "_" + file["id"]
  file_md5 = hashlib.md5( file_name + file_format + file_id).hexdigest()

  {
  "*data_type": file_type, 
  "filesets": {
    "submitter_id": fileset_submitter_id
  }, 
  "*consent": c, 
  "core_metadata_collections": {
    "submitter_id": cmc_submitter_id
  }, 
  "*type": "aggregate_level_data_file", 
  "*file_path": file_path, 
  "*data_format": file_format, 
  "*md5sum": file_md5, 
  "*file_size": file_size, 
  "*submitter_id": file_submitter_id, 
  "*file_name": file_name
}