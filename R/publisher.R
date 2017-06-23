VIEWS_API_PATH <- "/api/views.json"
PUBLISHING_API_PATH <- "/api/publishing/v1"

#' Create a view
#'
#' Create a new view (dataset) with associated 4x4
#' @param name Dataset name Dataset name
#' @param description Dataset description
#' @param domain Socrata domain
#' @param username Username (email address) to be used for authentication
#' @param password Password to be used for authentication
#' @return HTTP response
#' @author Peter Schmiedeskamp \email{peter.schmiedeskamp@@socrata.com}
#' @noRd
create_view <- function(name, description, domain, username, password) {
    u <- httr::modify_url("", scheme = "https", hostname = domain,
                      path = VIEWS_API_PATH)

    body <- list(
        name = name,
        description = description
    )
    
    r <- httr::POST(u, body = body, httr::authenticate(username, password),
                    encode = "json")

    assertthat::assert_that(round(httr::status_code(r), -2) == 200,
                            msg = "View creation failed")

    r
}


#' Create a revision
#'
#' Create a new revision to an existing view (dataset)
#' @param action "replace" or "update"
#' @param fourfour four-by-four id of dataset
#' @param domain Socrata domain
#' @param username Username (email address) to be used for authentication
#' @param password Password to be used for authentication
#' @return HTTP response
#' @author Peter Schmiedeskamp \email{peter.schmiedeskamp@@socrata.com}
#' @noRd
create_revision <- function(action, fourfour, domain, username, password) {
    assertthat::assert_that(action %in% c("replace", "update", "metadata"))
    
    u <- httr::modify_url("", scheme = "https", hostname = domain,
                      path = c(PUBLISHING_API_PATH, "revision", fourfour))

    body <- list(action = list(type = action))
    
    r <- httr::POST(u, body = body, httr::authenticate(username, password),
                    encode = "json")

    assertthat::assert_that(round(httr::status_code(r), -2) == 200,
                            msg = "Revision creation failed")

    r
}


#' Apply a revision
#'
#' Apply a revision and commit changes
#' @param apply_path Returned from API
#' @param schema_id Returned from API
#' @param domain Socrata domain
#' @param username Username (email address) to be used for authentication
#' @param password Password to be used for authentication
#' @return HTTP response
#' @author Peter Schmiedeskamp \email{peter.schmiedeskamp@@socrata.com}
#' @noRd
apply_revision <- function(apply_path, schema_id, domain, username, password) {
    u <- httr::modify_url("", scheme = "https", hostname = domain, path = apply_path)

    body <- list(output_schema_id = schema_id)
    
    r <- httr::PUT(u, body = body, httr::authenticate(username, password),
                   encode = "json")

    assertthat::assert_that(round(httr::status_code(r), -2) == 200,
                            msg = "Applying revision failed")
}


#' Create a source
#'
#' Create an empty source that can subsequently be populated by a data upload  
#' @param filename Filename to register with web API
#' @param upload_path Returned from API
#' @param domain Socrata domain
#' @param username Username (email address) to be used for authentication
#' @param password Password to be used for authentication
#' @return HTTP response
#' @author Peter Schmiedeskamp \email{peter.schmiedeskamp@@socrata.com}
#' @noRd
create_source <- function(filename, upload_path,
                          domain, username, password,
                          fourfour, revision_seq ) { # fourfour and
                                                     # revision_seq
                                                     # will not be
                                                     # needed soon
    u <- httr::modify_url("", scheme = "https", hostname = domain,
                      path = upload_path)

    body <- list(
        filename = filename,
        fourfour = fourfour,
        revision_seq = revision_seq
        )
 
    r <- httr::POST(u, body = body, httr::authenticate(username, password),
                    encode = "json")

    assertthat::assert_that(round(httr::status_code(r), -2) == 200,
                            msg = "Source creation failed")

    r
}


#' Upload data to a source
#'
#' Populates a source with uploaded data
#' @param filepath Path to file to use as upload
#' @param mimetype Mimetype of file upload
#' @param bytes_path Returned from web API
#' @param domain Socrata domain
#' @param username Username (email address) to be used for authentication
#' @param password Password to be used for authentication
#' @return HTTP response
#' @author Peter Schmiedeskamp \email{peter.schmiedeskamp@@socrata.com}
#' @noRd
upload_to_source <- function(filepath, mimetype, bytes_path,
                        domain, username, password) {
    u <- httr::modify_url("", scheme = "https", hostname=domain, path = bytes_path)

    r <- httr::POST(u, body = httr::upload_file(filepath, mimetype),
              httr::authenticate(username, password))

    assertthat::assert_that(round(httr::status_code(r), -2) == 200,
                            msg = "Filling upload failed")

    r
}

#' Publish a dataframe to Socrata
#'
#'
#' @param dataframe A dataframe to publish
#' @param name Dataset name; ignored if overwriting an existing dataset
#' @param description Dataset description; ignored if overwriting existing dataset
#' @param domain Socrata domain
#' @param username Username (email address) to be used for authentication
#' @param password Password to be used for authentication
#' @return URL string pointing to newly published dataset
#' @author Peter Schmiedeskamp \email{peter.schmiedeskamp@@socrata.com}
#' @export
write_socrata <- function(dataframe, fourfour = NULL, name = NULL,
                          description = NULL, domain, username, password) {
    ## Serialize our dataframe to a temp CSV file
    filepath <- tempfile(fileext = ".csv")
    readr::write_csv(dataframe, filepath)

    if(is.null(name)) {
        name <- deparse(substitute(dataframe))
    }

    if(is.null(description)) {
        description <- name
    }

    
    if(is.null(fourfour)) {
        ## Create our view and extract the 4x4
        resp_view <- create_view(name, description, domain, username, password)
        fourfour <- httr::content(resp_view)$id
    }

    ## Create a revision, extract the upload path, apply_path, and revision_seq
    resp_rev  <- create_revision("replace", fourfour, domain, username, password)
    upload_path <- httr::content(resp_rev)$links$create_upload
    apply_path <- httr::content(resp_rev)$links$apply
    revision_seq <- httr::content(resp_rev)$resource$revision_seq

    ## Create an upload, extract the bytes path, first / only output_schema_id
    resp_src <- create_source(basename(filepath), upload_path, domain,
                              username, password, fourfour, revision_seq)
    bytes_path <- httr::content(resp_src)$links$bytes

    ## Push data to the upload
    resp_upload <- upload_to_source(filepath, "text/csv", bytes_path,
                                    domain, username, password)
    schema_id <- httr::content(resp_upload)$resource$output_schemas[[1]]$id
    
    ## Apply the revision
    apply_revision(apply_path, schema_id, domain, username, password)

    ## If all succeeded, return url of resource
    httr::modify_url("", scheme = "https", hostname = domain,
                     path = c("resource", fourfour))
    
}

