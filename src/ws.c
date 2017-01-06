		/*
 * This file is part of the KNOT Project
 *
 * Copyright (c) 2015, CESAR. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of the CESAR nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL CESAR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 * OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <stdint.h>
#include <errno.h>

#include <libwebsockets.h>

#include <glib.h>

#include <json-c/json.h>

#include "log.h"
#include "proto.h"

#define MAX_PAYLOAD		4096
#define SERVICE_TIMEOUT		100
#define IDENTIFY_REQUEST	"42[\"identify\"]"
#define READY_RESPONSE		"42[\"ready\""
#define NOT_READY_RESPONSE	"42[\"notReady\""
#define CONFIG_MSG		"42[\"config\",{\"0\":"
#define READY_RESPONSE_LEN	sizeof(READY_RESPONSE) - 1
#define NOT_READY_RESPONSE_LEN	sizeof(NOT_READY_RESPONSE) - 1
#define CONFIG_MSG_LEN		sizeof(CONFIG_MSG) - 1
#define CLOUD_PATH		"/socket.io/?EIO=4&transport=websocket"
#define DEFAULT_CLOUD_HOST	"localhost"
#define DEVICE_INDEX		0
#define REGISTER_PACKET_TYPE	420
#define MESSAGE_PREFIX		42

struct lws_context *context;
static GHashTable *wstable;
gboolean got_response = FALSE;
gboolean connection_error = FALSE;
gboolean connected = FALSE;
static gboolean ready = FALSE;
struct lws_client_connect_info info;
static char *host_address = "localhost";
static int host_port = 3000;

/* Struct used to fetch data from cloud and send to THING */
struct to_fetch {
	int proto_sock;
	char uuid[36+1];	/* UUID + '\0' */
	char token[40+1];	/* TOKEN + '\0' */
	void *user_data;
	void (*watch_cb)(json_raw_t, void *);
};

struct per_session_data_ws {
	struct lws *ws;
	/*
	 * This buffer MUST have LWS_PRE bytes valid BEFORE the pointer. this
	 * is defined in the lws documentation,
	 */
	unsigned char buffer[LWS_PRE + MAX_PAYLOAD];
	unsigned int len;
	char *json;
	struct to_fetch data;
};

/*
 * A received message has the following structure:
 * <packet_type>[json_message]
 * Packet types defined by Engine.IO:
 */
enum packet_type {
	EIO_OPEN,
	EIO_CLOSE,
	EIO_PING,
	EIO_PONG,
	EIO_MSG,
	EIO_UPGRADE,
	EIO_NOOP
};

struct handshake_data {
	const char *sid;
	int pingInterval;
	int pingTimeout;
};

static struct handshake_data *h_data;
static struct per_session_data_ws *psd;

static gboolean timeout_ws(gpointer user_data)
{
	lws_service(context, SERVICE_TIMEOUT);

	return TRUE;
}
/*
 * Return '0' if device has been created or a negative value
 * mapped to generic Linux -errno codes.
 */
static int ret2errno(const char *json_str, const char *expected_result)
{
	json_object *jobj, *jobjentry;
	int err = -EIO;

	jobj = json_tokener_parse(json_str);
	if (jobj == NULL)
		goto done;

	if (json_object_get_type(jobj) == json_type_array) {
		jobjentry = json_object_array_get_idx(jobj, DEVICE_INDEX);
		if (jobjentry == NULL)
			goto done;
	}
	if (!strcmp(json_object_get_string(jobjentry), expected_result))
		err = 0;

done:
	json_object_put(jobj);

	return err;
}
static int handle_response(json_raw_t *json, char *property)
{
	size_t realsize;
	json_object *jobj, *jres, *jprop;
	const char *jobjstringres;

	jres = json_tokener_parse(psd->json);
	if (jres == NULL)
		return -EINVAL;

	jobj = json_object_array_get_idx(jres, DEVICE_INDEX);

	jobjstringres = json_object_to_json_string(jobj);
	if (property) {
		json_object_object_get_ex(jobj, property, &jprop);
		jobjstringres = json_object_to_json_string(jprop);
	}

	realsize = strlen(jobjstringres) + 1;

	json->data = (char *) realloc(json->data, json->size + realsize);
	if (json->data == NULL) {
		LOG_ERROR("Not enough memory\n");
		return -ENOMEM;
	}

	memcpy(json->data + json->size, jobjstringres, realsize);
	json->size += realsize;
	json->data[json->size - 1] = 0;
	json_object_put(jres);

	return 0;
}

static void parse_handshake_data(char *json_str)
{
	json_object *jobj, *jsid, *jtimeout, *jinterval;

	jobj = json_tokener_parse(json_str);

	if (!json_object_object_get_ex(jobj, "sid", &jsid))
		goto done;
	if (!json_object_object_get_ex(jobj, "pingInterval", &jinterval))
		goto done;
	if (!json_object_object_get_ex(jobj, "pingTimeout", &jtimeout))
		goto done;

	h_data = g_new0(struct handshake_data, 1);

	h_data->sid = json_object_get_string(jsid);
	h_data->pingInterval = json_object_get_int(jinterval);
	h_data->pingTimeout = json_object_get_int(jtimeout);

	/* TODO: Send ping every h_data.pingInterval; */
done:
	g_free(h_data);
	json_object_put(jobj);
}

static const char *lws_reason2str(enum lws_callback_reasons reason)
{
	switch (reason) {
	case LWS_CALLBACK_ESTABLISHED:
		return "ESTABLISHED";
	case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
		return "CLIENT_CONNECTION_ERROR";
	case LWS_CALLBACK_CLIENT_FILTER_PRE_ESTABLISH:
		return "CLIENT_FILTER_PRE_ESTABLISH";
	case LWS_CALLBACK_CLIENT_ESTABLISHED:
		return "CLIENT_ESTABLISHED";
	case LWS_CALLBACK_CLOSED:
		return "CLOSED";
	case LWS_CALLBACK_CLOSED_HTTP:
		return "CLOSED_HTTP";
	case LWS_CALLBACK_RECEIVE:
		return "RECEIVE";
	case LWS_CALLBACK_CLIENT_RECEIVE:
		return "CLIENT_RECEIVE";
	case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
		return "CLIENT_RECEIVE_PONG";
	case LWS_CALLBACK_CLIENT_WRITEABLE:
		return "CLIENT_WRITEABLE";
	case LWS_CALLBACK_SERVER_WRITEABLE:
		return "SERVER_WRITEABLE";
	case LWS_CALLBACK_HTTP:
		return "HTTP";
	case LWS_CALLBACK_HTTP_BODY:
		return "HTTP_BODY";
	case LWS_CALLBACK_HTTP_BODY_COMPLETION:
		return "BODY_COMPLETION";
	case LWS_CALLBACK_HTTP_FILE_COMPLETION:
		return "HTTP_FILE_COMPLETION";
	case LWS_CALLBACK_HTTP_WRITEABLE:
		return "HTTP_WRITEABLE";
	case LWS_CALLBACK_FILTER_NETWORK_CONNECTION:
		return "FILTER_NETWORK_CONNECTION";
	case LWS_CALLBACK_FILTER_HTTP_CONNECTION:
		return "FILTER_HTTP_CONNECTION";
	case LWS_CALLBACK_SERVER_NEW_CLIENT_INSTANTIATED:
		return "SERVER_NEW_CLIENT_INSTANTIATED";
	case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION:
		return "FILTER_PROTOCOL_CONNECTION";
	case LWS_CALLBACK_OPENSSL_LOAD_EXTRA_CLIENT_VERIFY_CERTS:
		return "OPENSSL_LOAD_EXTRA_CLIENT_VERIFY_CERTS";
	case LWS_CALLBACK_OPENSSL_LOAD_EXTRA_SERVER_VERIFY_CERTS:
		return "OPENSSL_LOAD_EXTRA_SERVER_VERIFY_CERTS";
	case LWS_CALLBACK_OPENSSL_PERFORM_CLIENT_CERT_VERIFICATION:
		return "OPENSSL_PERFORM_CLIENT_CERT_VERIFICATION";
	case LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER:
		return "CLIENT_APPEND_HANDSHAKE_HEADER";
	case LWS_CALLBACK_CONFIRM_EXTENSION_OKAY:
		return "CONFIRM_EXTENSION_OKAY";
	case LWS_CALLBACK_CLIENT_CONFIRM_EXTENSION_SUPPORTED:
		return "CLIENT_CONFIRM_EXTENSION_SUPPORTED";
	case LWS_CALLBACK_PROTOCOL_INIT:
		return "PROTOCOL_INIT";
	case LWS_CALLBACK_PROTOCOL_DESTROY:
		return "PROTOCOL_DESTROY";
	case LWS_CALLBACK_WSI_CREATE:
		return "WSI_CREATE";
	case LWS_CALLBACK_WSI_DESTROY:
		return "WSI_DESTROY";
	case LWS_CALLBACK_GET_THREAD_ID:
		return "GET_THREAD_ID";

	case LWS_CALLBACK_ADD_POLL_FD:
		return "ADD_POLL_FD";
	case LWS_CALLBACK_DEL_POLL_FD:
		return "DEL_POLL_FD";
	case LWS_CALLBACK_CHANGE_MODE_POLL_FD:
		return "CHANGE_MODE_POLL_FD";
	case LWS_CALLBACK_LOCK_POLL:
		return "LOCK_POLL";
	case LWS_CALLBACK_UNLOCK_POLL:
		return "UNLOCK_POLL";

	case LWS_CALLBACK_OPENSSL_CONTEXT_REQUIRES_PRIVATE_KEY:
		return "OPENSSL_CONTEXT_REQUIRES_PRIVATE_KEY";
	case LWS_CALLBACK_USER:
		return "USER";

	case LWS_CALLBACK_RECEIVE_PONG:
		return "RECEIVE PONG";
	case LWS_CALLBACK_WS_PEER_INITIATED_CLOSE:
		return "PEER INITIATED CLOSE";
	case LWS_CALLBACK_WS_EXT_DEFAULTS:
		return "EXT DEFAULTS";
	case LWS_CALLBACK_CGI:
		return "CGI";
	case LWS_CALLBACK_CGI_TERMINATED:
		return "CGI TERMINATED";
	case LWS_CALLBACK_CGI_STDIN_DATA:
		return "CGI STDIN DATA";
	case LWS_CALLBACK_CGI_STDIN_COMPLETED:
		return "CGI STDIN COMPLETED";
	case LWS_CALLBACK_ESTABLISHED_CLIENT_HTTP:
		return "ESTABLISHED CLIENT HTTP";
	case LWS_CALLBACK_CLOSED_CLIENT_HTTP:
		return "CLOSED CLIENT HTTP";
	case LWS_CALLBACK_RECEIVE_CLIENT_HTTP:
		return "RECEIVE CLIENT HTTP";
	case LWS_CALLBACK_COMPLETED_CLIENT_HTTP:
		return "COMPLETED CLIENT HTTP";
	case LWS_CALLBACK_RECEIVE_CLIENT_HTTP_READ:
		return "RECEIVE CLIENT HTTP READ";

	case LWS_CALLBACK_HTTP_DROP_PROTOCOL:
		return "LWS_CALLBACK_HTTP_DROP_PROTOCOL";
	case LWS_CALLBACK_CHECK_ACCESS_RIGHTS:
		return "LWS_CALLBACK_CHECK_ACCESS_RIGHTS";
	case LWS_CALLBACK_PROCESS_HTML:
		return "LWS_CALLBACK_PROCESS_HTML";
	case LWS_CALLBACK_ADD_HEADERS:
		return "LWS_CALLBACK_ADD_HEADERS";
	case LWS_CALLBACK_SESSION_INFO:
		return "LWS_CALLBACK_SESSION_INFO";
	case LWS_CALLBACK_GS_EVENT:
		return "LWS_CALLBACK_GS_EVENT";
	case LWS_CALLBACK_HTTP_PMO:
		return "LWS_CALLBACK_HTTP_PMO";
	case LWS_CALLBACK_CLIENT_HTTP_WRITEABLE:
		return "LWS_CALLBACK_CLIENT_HTTP_WRITEABLE";
	case LWS_CALLBACK_HTTP_BIND_PROTOCOL:
		return "LWS_CALLBACK_HTTP_BIND_PROTOCOL";

	default:
		return "UNKNOWN";
	}
}

static void ws_close(int sock)
{
	LOG_INFO("\nWS CLOSE: %d\n", sock);
	if (!g_hash_table_remove(wstable, GINT_TO_POINTER(sock)))
		LOG_ERROR("Removing key: sock %d not found!\n", sock);
}
static int ws_mknode(int sock, const char *device_json,
					json_raw_t *json)
{
	int err;
	json_object *jobj, *jarray;
	const char *jobjstring;

	jobj = json_tokener_parse(device_json);
	if (jobj == NULL)
		return -EINVAL;

	jarray = json_object_new_array();
	json_object_array_add(jarray, json_object_new_string("register"));
	json_object_array_add(jarray, jobj);
	jobjstring = json_object_to_json_string(jarray);

	psd = g_hash_table_lookup(wstable, GINT_TO_POINTER(sock));

	if (psd->ws == NULL) {
		err = -EBADF;
		LOG_ERROR("Not found\n");
		goto done;
	}
	psd->len = sprintf((char *) psd->buffer + LWS_PRE, "%d%s",
					REGISTER_PACKET_TYPE, jobjstring);
	lws_callback_on_writable(psd->ws);

	/* Keep serving context until server responds or an error occurs */
	while (!got_response && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);

	if (connection_error)
		err = -ECONNRESET;

	err = handle_response(json, NULL);

	if (err < 0)
		goto done;
done:
	connection_error = FALSE;
	got_response = FALSE;

	json_object_put(jarray);

	return err;
}

static int ws_device(int sock, const char *uuid,
					const char *token, json_raw_t *json)
{
	int err;
	const char *jobjstring;
	json_object *jobj, *jarray;

	jobj = json_object_new_object();
	jarray = json_object_new_array();

	if (!jobj || !jarray) {
		LOG_ERROR("JSON: no memory\n");
		err = -ENOMEM;
		return err;
	}

	json_object_object_add(jobj, "uuid", json_object_new_string(uuid));

	json_object_array_add(jarray,
	json_object_new_string("device"));
	json_object_array_add(jarray, jobj);

	jobjstring = json_object_to_json_string(jarray);

	LOG_INFO("TX JSON %s\n", jobjstring);

	psd = g_hash_table_lookup(wstable, GINT_TO_POINTER(sock));

	if (psd->ws == NULL) {
		LOG_ERROR("Not found\n");
		err = -EBADF;
		goto done;
	}

	psd->len = sprintf((char *)&psd->buffer + LWS_PRE, "%d%s",
						REGISTER_PACKET_TYPE, jobjstring);
	lws_callback_on_writable(psd->ws);

	while (!got_response && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);

	if (connection_error) {
		err = -ECONNREFUSED;
		goto done;
	}

	err = handle_response(json, "device");

done:
	got_response = FALSE;
	connection_error = FALSE;

	json_object_put(jarray);

	return err;
}

static int ws_signin(int sock, const char *uuid, const char *token,
							json_raw_t *json)
{
	int err;
	const char *jobjstring;
	json_object *jobj, *jarray;

	jobj = json_object_new_object();
	jarray = json_object_new_array();

	if (!jobj || !jarray) {
		LOG_ERROR("JSON: no memory\n");
		err = -ENOMEM;
		return err;
	}

	json_object_object_add(jobj, "uuid", json_object_new_string(uuid));
	json_object_object_add(jobj, "token", json_object_new_string(token));

	json_object_array_add(jarray,
	json_object_new_string("identity"));
	json_object_array_add(jarray, jobj);

	jobjstring = json_object_to_json_string(jarray);

	LOG_INFO("TX JSON %s\n", jobjstring);

	psd = g_hash_table_lookup(wstable, GINT_TO_POINTER(sock));

	if (psd->ws == NULL) {
		LOG_ERROR("Not found\n");
		err = -EBADF;
		goto done;
	}

	psd->len = sprintf((char *)&psd->buffer + LWS_PRE, "%d%s",
						MESSAGE_PREFIX, jobjstring);
	lws_callback_on_writable(psd->ws);

	/* Keep serving context until server responds or an error occurs */
	while (!ready && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);

	if (connection_error) {
		err = -ECONNREFUSED;
		goto done;
	}

	err = ws_device(sock, uuid, token, json);

done:
	ready = FALSE;
	connection_error = FALSE;

	json_object_put(jarray);

	return err;
}

static int ws_rmnode(int sock, const char *uuid, const char *token,
							json_raw_t *json)
{
	int err;
	const char *jobjstring;
	json_object *jobj, *jarray;

	jobj = json_object_new_object();
	jarray = json_object_new_array();

	if (!jobj || !jarray) {
		LOG_ERROR("JSON: no memory\n");
		return -ENOMEM;
	}

	json_object_object_add(jobj, "uuid", json_object_new_string(uuid));
	json_object_object_add(jobj, "token", json_object_new_string(token));

	jobjstring = json_object_to_json_string(jobj);

	json_object_array_add(jarray, json_object_new_string("unregister"));
	json_object_array_add(jarray, jobj);

	jobjstring = json_object_to_json_string(jarray);

	LOG_INFO("TX JSON %s\n", jobjstring);

	psd = g_hash_table_lookup(wstable, GINT_TO_POINTER(sock));

	if (psd->ws == NULL) {
		LOG_ERROR("Not found\n");
		err = -EBADF;
		goto done;
	}

	psd->len = sprintf((char *)&psd->buffer + LWS_PRE, "%d%s",
		 			REGISTER_PACKET_TYPE, jobjstring);
	lws_callback_on_writable(psd->ws);

	/* Keep serving context until server responds or an error occurs */
	while (!got_response && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);

	if (connection_error)
		err = -ECONNRESET;

	err = handle_response(json, NULL);

	if (err < 0)
		goto done;
done:
	got_response = FALSE;
	connection_error = FALSE;

	json_object_put(jarray);

	return err;
}

static int ws_schema(int sock, const char *uuid, const char *token,
					const char *jreq, json_raw_t *json)
{
	int err;
	struct json_object *jobj, *ajobj, *jobjdev, *jarray;
	const char *jobjstr, *expected_result = "config";

	jobj = json_tokener_parse(jreq);
	if (jobj == NULL)
		return -EINVAL;

	ajobj = json_object_new_array();
	jarray = json_object_new_array();
	jobjdev = json_object_new_object();

	json_object_array_add(jarray, json_object_new_string("update"));
	json_object_object_add(jobjdev, "uuid", json_object_new_string(uuid));
	json_object_object_add(jobjdev, "token", json_object_new_string(token));
	json_object_array_add(ajobj, jobjdev);

	json_object_array_add(ajobj, jobj);
	json_object_array_add(jarray, ajobj);
	jobjstr = json_object_to_json_string(jarray);

	psd = g_hash_table_lookup(wstable, GINT_TO_POINTER(sock));

	if (psd->ws == NULL) {
		LOG_ERROR("Not found\n");
		err = -EBADF;
		goto done;
	}

	psd->len = sprintf((char *)&psd->buffer + LWS_PRE, "%d%s",
						MESSAGE_PREFIX, jobjstr);
	lws_callback_on_writable(psd->ws);

	/* Keep serving context until server responds or an error occurs */
	while (!got_response && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);

	if (connection_error) {
		err = -ECONNREFUSED;
		goto done;
	}

	err = ret2errno(psd->json, expected_result);

done:
	got_response = FALSE;
	connection_error = FALSE;

	json_object_put(jarray);

	return err;
}

static int ws_data(int sock, const char *uuid, const char *token,
					const char *jreq, json_raw_t *json)
{
	int err;
	struct json_object *jobj, *jmsg;
	const char *jobjstr;

	jobj = json_tokener_parse(jreq);
	if (jobj == NULL)
		return -EINVAL;

	jmsg = json_object_new_array();
	json_object_array_add(jmsg, json_object_new_string("data"));
	json_object_object_add(jobj, "uuid", json_object_new_string(uuid));
	json_object_object_add(jobj, "token", json_object_new_string(token));
	json_object_array_add(jmsg, jobj);
	jobjstr = json_object_to_json_string(jmsg);

	psd = g_hash_table_lookup(wstable, GINT_TO_POINTER(sock));

	if (psd->ws == NULL) {
		LOG_ERROR("Not found\n");
		err = -EBADF;
		goto done;
	}
	psd->len = sprintf((char *)&psd->buffer[LWS_PRE], "%d%s",
						REGISTER_PACKET_TYPE, jobjstr);
	lws_callback_on_writable(psd->ws);
	err = 0;

	while (!got_response && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);


done:
	got_response = FALSE;
	connection_error = FALSE;

	json_object_put(jmsg);

	return err;
}

static int send_identity(void)
{
	int err = -ECONNREFUSED;
	/*
	 * An identity msg with an empty json will generate a new uuid/token
	 * a possible FIXME is to authenticate the owner (GW)
	 */
	psd->len = sprintf((char *) psd->buffer + LWS_PRE,
							"42[\"identity\",{}]");
	lws_callback_on_writable(psd->ws);

	while (!ready && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);

	if (connection_error)
		goto done;

	err = 0;
	ready = FALSE;
	connected = TRUE;
	got_response = FALSE;
done:

	return err;
}

static void handle_cloud_response(char *resp, struct lws *wsi)
{
	char dest[MAX_PAYLOAD];
	int len = strlen(resp);
	int i;
	int packet_type = resp[0] - '0';

	LOG_INFO("JSON_RX %d = %s\n", len, resp);

	switch (packet_type) {
	case EIO_OPEN:
		memset(dest, '\0', sizeof(dest));
		/* Ignore packet type and parse handshake data */
		strncpy(dest, resp + 1, len - 1);
		parse_handshake_data(dest);
		break;
	case EIO_PONG:
		/* TODO */
		break;
	case EIO_MSG:
		if (!strcmp(resp, IDENTIFY_REQUEST)) {
			connected = TRUE;
		} else if (!strncmp(resp, READY_RESPONSE,
						READY_RESPONSE_LEN)) {
			ready = TRUE;
		} else if (!strncmp(resp, NOT_READY_RESPONSE,
					NOT_READY_RESPONSE_LEN)) {
			connection_error = TRUE;
		} else if (!strncmp(resp, CONFIG_MSG, CONFIG_MSG_LEN)) {
			json_raw_t json;
			size_t realsize;
			json_object *jobj, *jres;
			const char *jobjstringres;
			struct per_session_data_ws *session_data;
			i = 0;

			session_data = g_hash_table_lookup(wstable,
				GINT_TO_POINTER(lws_get_socket_fd(wsi)));

			if(!session_data->ws)
				break;

			while (resp[i] != '[' && i < len)
				i++;

			memset(&json, 0, sizeof(json_raw_t));

			jres = json_tokener_parse(resp + i);
			if (jres == NULL)
				break;

			jobj = json_object_array_get_idx(jres, 1);

			jobjstringres = json_object_to_json_string(jobj);

			realsize = strlen(jobjstringres) + 1;

			json.data = (char *) realloc(json.data, json.size + realsize);
			if (json.data == NULL) {
				LOG_ERROR("Not enough memory\n");
				break;
			}

			memcpy(json.data + json.size, jobjstringres, realsize);
			json.size += realsize;
			json.data[json.size - 1] = 0;

			session_data->data.watch_cb(json,
						session_data->data.user_data);

			json_object_put(jres);
			free(json.data);
		} else {
			/*
			 * TODO: The actual data only begins after the
			 * '[' so, we need to parse it.
			 */
			i = 0;

			while (resp[i] != '[' && i < len)
				i++;
			/*
			 * FIXME: For now, we only consider responses
			 * like: * 430["..."]
			 */
			if (i > 1 && i < len) {
				psd->json = g_strndup(resp + i, len - i);
				got_response = TRUE;
			}
		}
		break;
	default:
		break;
	}

	g_free(resp);
}

static int callback_lws_http(struct lws *wsi,
					enum lws_callback_reasons reason,
					void *user_data, void *in, size_t len)

{
	char *rsp;
	LOG_INFO("reason(%02X): %s -- %p\n", reason, lws_reason2str(reason), user_data);

	switch (reason) {
	case LWS_CALLBACK_ESTABLISHED:
		LOG_INFO("LWS_CALLBACK_ESTABLISHED\n");
		break;
	case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
		LOG_INFO("LWS_CALLBACK_CLIENT_CONNECTION_ERROR\n");
		connection_error = TRUE;
		break;
	case LWS_CALLBACK_CLIENT_FILTER_PRE_ESTABLISH:
		break;
	case LWS_CALLBACK_CLIENT_ESTABLISHED:
		LOG_INFO("LWS_CALLBACK_CLIENT_ESTABLISHED\n");
		break;
	case LWS_CALLBACK_CLOSED:
		LOG_INFO("LWS_CALLBACK_CLOSED\n");
		connection_error = TRUE;
		break;
	case LWS_CALLBACK_CLOSED_HTTP:
		break;
	case LWS_CALLBACK_RECEIVE:
		break;
	case LWS_CALLBACK_CLIENT_RECEIVE:
		{
		rsp = g_strndup(in, len);
		handle_cloud_response(rsp, wsi);
		break;
		}
	case LWS_CALLBACK_CLIENT_RECEIVE_PONG:
		break;
	case LWS_CALLBACK_CLIENT_WRITEABLE:
		{
		int l;

		if (psd->ws == wsi)
			LOG_INFO("Client wsi %p writable\n", wsi);

		l = lws_write(psd->ws, &psd->buffer[LWS_PRE], psd->len,
								LWS_WRITE_TEXT);
		LOG_INFO("Wrote (%d) bytes\n", l);

		/* Enable RX when after message is successfully sent */
		if (l < 0) {
			connection_error = TRUE;
			return -1;
		}
			lws_rx_flow_control(wsi, 1);
		}
		break;
	case LWS_CALLBACK_SERVER_WRITEABLE:
	case LWS_CALLBACK_HTTP:
	case LWS_CALLBACK_HTTP_BODY:
	case LWS_CALLBACK_HTTP_BODY_COMPLETION:
	case LWS_CALLBACK_HTTP_FILE_COMPLETION:
	case LWS_CALLBACK_HTTP_WRITEABLE:
	case LWS_CALLBACK_FILTER_NETWORK_CONNECTION:
	case LWS_CALLBACK_FILTER_HTTP_CONNECTION:
	case LWS_CALLBACK_SERVER_NEW_CLIENT_INSTANTIATED:
	case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION:
	case LWS_CALLBACK_OPENSSL_LOAD_EXTRA_CLIENT_VERIFY_CERTS:
	case LWS_CALLBACK_OPENSSL_LOAD_EXTRA_SERVER_VERIFY_CERTS:
	case LWS_CALLBACK_OPENSSL_PERFORM_CLIENT_CERT_VERIFICATION:
	case LWS_CALLBACK_CONFIRM_EXTENSION_OKAY:
	case LWS_CALLBACK_CLIENT_CONFIRM_EXTENSION_SUPPORTED:
	case LWS_CALLBACK_PROTOCOL_INIT:
	case LWS_CALLBACK_PROTOCOL_DESTROY:
	case LWS_CALLBACK_WSI_CREATE: // always protocol[0]
	case LWS_CALLBACK_WSI_DESTROY: // always protocol[0]
	case LWS_CALLBACK_GET_THREAD_ID:
	case LWS_CALLBACK_ADD_POLL_FD:
	case LWS_CALLBACK_DEL_POLL_FD:
	case LWS_CALLBACK_CHANGE_MODE_POLL_FD:
	case LWS_CALLBACK_LOCK_POLL:
	case LWS_CALLBACK_UNLOCK_POLL:
	case LWS_CALLBACK_OPENSSL_CONTEXT_REQUIRES_PRIVATE_KEY:
	case LWS_CALLBACK_USER:
	case LWS_CALLBACK_RECEIVE_PONG:
	case LWS_CALLBACK_WS_PEER_INITIATED_CLOSE:
	case LWS_CALLBACK_WS_EXT_DEFAULTS:
	case LWS_CALLBACK_CGI:
	case LWS_CALLBACK_CGI_TERMINATED:
	case LWS_CALLBACK_CGI_STDIN_DATA:
	case LWS_CALLBACK_CGI_STDIN_COMPLETED:
	case LWS_CALLBACK_ESTABLISHED_CLIENT_HTTP:
	case LWS_CALLBACK_CLOSED_CLIENT_HTTP:
	case LWS_CALLBACK_RECEIVE_CLIENT_HTTP:
	case LWS_CALLBACK_COMPLETED_CLIENT_HTTP:
	case LWS_CALLBACK_RECEIVE_CLIENT_HTTP_READ:
	case LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER:
	case LWS_CALLBACK_HTTP_DROP_PROTOCOL:
	case LWS_CALLBACK_CHECK_ACCESS_RIGHTS:
	case LWS_CALLBACK_PROCESS_HTML:
	case LWS_CALLBACK_ADD_HEADERS:
	case LWS_CALLBACK_SESSION_INFO:
	case LWS_CALLBACK_GS_EVENT:
	case LWS_CALLBACK_HTTP_PMO:
	case LWS_CALLBACK_CLIENT_HTTP_WRITEABLE:
	case LWS_CALLBACK_HTTP_BIND_PROTOCOL:
		break;
	default:
		break;
	}

	return 0;
}

static struct lws_protocols protocols[] = {
	{
		"http-only",
		callback_lws_http,
		0,
	},
	{
		NULL, NULL, 0/* end of list */
	}
};

static int ws_connect(void)
{
	struct lws_client_connect_info info;
	int err;
	static char ads_port[300];
	gboolean use_ssl = FALSE; /* wss */
	int sock;

	memset(&info, 0, sizeof(info));
	snprintf(ads_port, sizeof(ads_port), "%s:%u", host_address, host_port);

	LOG_INFO("Connecting to %s...\n", ads_port);

	psd = g_try_new0(struct per_session_data_ws, 1);

	info.context = context;
	info.ssl_connection = use_ssl;
	info.address = host_address;
	info.port = host_port;
	info.path = CLOUD_PATH;
	info.host = info.address;
	info.origin = info.address;
	info.ietf_version_or_minus_one = -1;
	info.protocol = protocols[0].name;
	psd->ws = lws_client_connect_via_info(&info);

	if (psd->ws == NULL) {
		int err = errno;
		LOG_ERROR("libwebsocket_client_connect(): %s(%d)\n",
							strerror(err), err);
		return -err;
	}

	while (!connected && !connection_error)
		lws_service(context, SERVICE_TIMEOUT);

	sock = lws_get_socket_fd(psd->ws);
	g_hash_table_insert(wstable, GINT_TO_POINTER(sock), psd);

	connected = FALSE;
	connection_error = FALSE;
	got_response = FALSE;

	err = send_identity();
	if (err < 0)
		return err;
	connected = FALSE;
	connection_error = FALSE;
	got_response = FALSE;

	/* FIXME: Investigate alternatives for libwebsocket_service() */
	g_timeout_add_seconds(1, timeout_ws, NULL);

	return sock;
}

static void custom_free(gpointer userdata)
{
	struct per_session_data_ws *psd = userdata;

	LOG_INFO("\n%p\n", psd);
	g_free(psd->json);
	g_free(psd);
}

static int ws_probe(const char *host, unsigned int port)
{
	struct lws_context_creation_info i;
	memset(&i, 0, sizeof i);

	i.port = CONTEXT_PORT_NO_LISTEN;
	i.gid = -1;
	i.uid = -1;
	i.protocols = protocols;
	context = lws_create_context(&i);
	wstable = g_hash_table_new_full(g_direct_hash, g_direct_equal,
							NULL, custom_free);

	return 0;
}

static void ws_remove(void)
{
	g_hash_table_destroy(wstable);
	lws_context_destroy(context);
}

/*
 * Watch or poll the cloud to changes in the device.
 */
static unsigned int proto_register_watch(int proto_sock, const char *uuid,
				const char *token, void (*proto_watch_cb)
				(json_raw_t, void *), void *user_data)
{
	struct to_fetch *data;
	struct per_session_data_ws *value;

	data = g_new0(struct to_fetch, 1);

	value = g_hash_table_lookup(wstable, GINT_TO_POINTER(proto_sock));

	memcpy(data->uuid, uuid, sizeof(data->uuid));
	memcpy(data->token, token, sizeof(data->token));
	data->watch_cb = proto_watch_cb;
	data->user_data = user_data;

	value->data = *data;
	g_hash_table_insert(wstable, GINT_TO_POINTER(proto_sock), value);

	g_free(data);

	return 0;
}

struct proto_ops proto_ws = {
	.name = "ws",	/* websockets */
	.probe = ws_probe,
	.remove = ws_remove,
	.connect = ws_connect,
	.close = ws_close,
	.mknode = ws_mknode,
	.signin = ws_signin,
	.rmnode = ws_rmnode,
	.schema = ws_schema,
	.data = ws_data,
	.fetch = ws_device,
	.async = proto_register_watch
};
