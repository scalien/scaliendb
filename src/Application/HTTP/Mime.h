#ifndef MIME_H
#define MIME_H

#define MIME_TYPE_TEXT_PLAIN				"text/plain"
#define MIME_TYPE_TEXT_HTML					"text/html"
#define MIME_TYPE_APPLICATION_OCTET_STREAM	"application/octet-stream"

/*
===============================================================================================

 Mime types.

===============================================================================================
*/

const char* MimeTypeFromExtension(const char* ext, const char* defaultType = MIME_TYPE_APPLICATION_OCTET_STREAM);

#endif
