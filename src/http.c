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
#include <string.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "proto.h"
#include "http.h"

#define MESHBLU_HOST		"meshblu.octoblu.com"

static struct in_addr host_addr;

static int http_connect(void)
{
	struct sockaddr_in server;
	int sock, err;

	/*
	 * TODO: At the moment connect is blocking. Does it make
	 * sense to use asynchronous communication or use fork/pthread?
	 */
	sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sock == -1) {
		err = errno;
		printf("Meshblu socket(): %s(%d)\n", strerror(err), err);
		return -err;
	}

	memset(&server, 0, sizeof(server));
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = host_addr.s_addr;
	server.sin_port = htons(80);

	if (connect(sock, (struct sockaddr *) &server, sizeof(server)) < 0){
		err = errno;
		printf("Meshblu connect(): %s(%d)\n", strerror(err), err);

		close(sock);
		return -err;
	}

	return sock;
}

static int http_signup(int sock, struct json_buffer *jbuf)
{
	return -ENOSYS;
}

static int http_signin(int sock, const char *token)
{
	return -ENOSYS;
}

static void http_close(int sock)
{

}

static struct proto_ops ops = {
	.connect = http_connect,
	.close = http_close,
	.signup = http_signup,
	.signin = http_signin,
};

int http_register(void)
{
	struct hostent *host;
	int err;

	/* TODO: connect and track TCP socket */

	/* TODO: Add timer if it fails? */
	host = gethostbyname(MESHBLU_HOST);
	if  (host == NULL) {
		err = errno;
		printf("gethostbyname(%s): %s (%d)\n", MESHBLU_HOST,
						       strerror(err), err);
		return -err;
	}

	host_addr.s_addr = *((unsigned long *) host-> h_addr_list[0]);

	fprintf(stdout, "Meshblu IP: %s\n", inet_ntoa(host_addr));

	return proto_ops_register(&ops);
}

void http_unregister(void)
{
	/* TODO: replace by a built-in plugin mechnism */

	proto_ops_unregister(&ops);
}
