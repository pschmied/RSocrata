#' DBI methods
#'
#' Implementations of functions defined in the `DBI` package. Cribbed
#' heavily from `bigrquery` package, and Hadley's DBI backend vignette
#' (https://rstats-db.github.io/DBI/articles/backend.html)
#' @name DBI
NULL

#' Driver for Socrata database.
#' 
#' @keywords internal
#' @export
#' @import DBI methods
setClass("SocrataDriver", contains = "DBIDriver")

#' @export
#' @rdname Socrata-class
setMethod("dbUnloadDriver", "SocrataDriver", function(drv, ...) {
  TRUE
})


#' @export
Socrata <- function() {
  new("SocrataDriver")
}


#' Socrata connection class.
#' 
#' @export
#' @keywords internal
setClass("SocrataConnection", 
  contains = "DBIConnection", 
  slots = list(
    domain = "character",
    username = "character",
    password = "character",
    token = "character"
  )
)

#' @param drv An object created by \code{Socrata()} 
#' @rdname Socrata
#' @export
#' @examples
#' \dontrun{
#' db <- dbConnect(RSocrata::Socrata())
#' dbWriteTable(db, "mtcars", mtcars)
#' dbGetQuery(db, "SELECT * WHERE cyl == 4")
#' }
setMethod("dbConnect", "SocrataDriver",
  function(drv, domain, username, password, token, ...) {
    new("SocrataConnection",
      domain = domain,
      username = username,
      password = password,
      token = token
    )
  }
)

#' @param drv An object created by \code{Socrata()} 
#' @rdname Socrata
#' @export
setMethod(
  "dbDisconnect", "SocrataConnection",
  function(conn, ...) {
    if (!dbIsValid(conn)) {
      warning("Connection already closed.", call. = FALSE)
    }

    unset_result(conn)
    conn@.envir$valid <- FALSE

    TRUE
  }
)

