#' Download if specified
#'
#' @param blob_path The name of the blob to download
#' @param blob_storage_container The name of the container to donwload from
#' @param dir The directory to which to write the downloaded file
#' @return The path of the file
#' @family azure
#' @export
download_if_specified <- function(
    blob_path,
    blob_storage_container,
    dir) {
  # Guard against null input erroring out file.exists()
  if (rlang::is_null(blob_path)) {
    local_path <- NULL
  } else {
    file_exists <- file.exists(file.path(dir, blob_path))
    if (!rlang::is_null(blob_storage_container) && !file_exists) {
      container <- fetch_blob_container(blob_storage_container)
      local_path <- download_file_from_container(
        blob_storage_path = blob_path,
        local_file_path = file.path(dir, blob_path),
        storage_container = container
      )
    } else {
      local_path <- file.path(dir, blob_path)
    }
  }
  local_path
}

#' Download specified blobs from Blob Storage and save them in a local dir
#'
#' @param blob_storage_path A character of a blob in `storage_container`
#' @param local_file_path The local path to save the blob
#' @param storage_container The blob storage container with `blob_storage_path`
#
#' @return Invisibly, `local_file_path`
#' @family azure
#' @export
download_file_from_container <- function(
    blob_storage_path,
    local_file_path,
    storage_container) {
  cli::cli_alert_info(
    "Downloading blob {.path {blob_storage_path}} to {.path {local_file_path}}"
  )

  rlang::try_fetch(
    {
      dirs <- dirname(local_file_path)

      if (!dir.exists(dirs)) {
        cli::cli_alert("Creating directory {.path {dirs}}")
        dir.create(dirs, recursive = TRUE)
      }

      AzureStor::download_blob(
        container = storage_container,
        src = blob_storage_path,
        dest = local_file_path,
        overwrite = TRUE
      )
    },
    error = function(cnd) {
      cli::cli_abort(c(
        "Failed to download {.path {blob_storage_path}}",
        ">" = "Does the blob exist in the container?"
      ))
    }
  )

  cli::cli_alert_success(
    "Blob {.path {blob_storage_path}} downloaded successfully"
  )

  invisible(local_file_path)
}

#' Load Azure Blob container using managed identity credentials
#'
#' As a result it is an impure function, and should be used bearing that
#' warning in mind.
#'
#' @param container_name The Azure Blob Storage container associated with the
#'   credentials
#' @return A Blob endpoint
#' @family azure
#' @export
fetch_blob_container <- function(container_name) {
  cli::cli_alert_info(
    "Attempting to connect to container {.var {container_name}}"
  )

  cli::cli_alert_info("Authenticating with managed identity credentials")
  rlang::try_fetch(
    {
      # First, get a general-purpose token using SP flow
      # Analogous to:
      # az login --identity
      token <- AzureAuth::get_managed_token(
        resource = "https://storage.azure.com"
      )
      # Then fetch a storage endpoint using the token. Follows flow from
      # https://github.com/Azure/AzureStor.
      # Note that we're using the ABS endpoint (the first example line)
      # but following the AAD token flow from the AAD alternative at
      # end of the box. If we didn't replace the endpoint and used the
      # example flow then it allows authentication to blob but throws
      # a 409 when trying to download.
      endpoint <- AzureStor::storage_endpoint(
        "https://cfaazurebatchprd.blob.core.windows.net",
        token = token
      )

      # Finally, set up instantiation of storage container generic
      container <- AzureStor::storage_container(endpoint, container_name)
    },
    error = function(cnd) {
      cli::cli_abort(
        "Failure authenticating connection to {.var {container_name}}",
        parent = cnd
      )
    }
  )

  cli::cli_alert_success("Authenticated connection to {.var {container_name}}")

  return(container)
}
