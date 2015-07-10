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
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <glib.h>

#include "manager.h"
#include "ws.h"
#include "http.h"

static GMainLoop *main_loop;

/* Default is websockets */
static const char *opt_proto = "ws";

static void sig_term(int sig)
{
	g_main_loop_quit(main_loop);
}

static GOptionEntry options[] = {
	{ "proto", 'p', 0, G_OPTION_ARG_STRING, &opt_proto,
					"protocol", "eg: http or ws" },
	{ NULL },
};

int main(int argc, char *argv[])
{
	GOptionContext *context;
	GError *gerr = NULL;
	int err;

	printf("KNOT Gateway\n");

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, options, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &gerr)) {
		printf("Invalid arguments: %s\n", gerr->message);
		g_error_free(gerr);
		g_option_context_free(context);
		exit(EXIT_FAILURE);
	}

	g_option_context_free(context);

	signal(SIGTERM, sig_term);
	signal(SIGINT, sig_term);

	main_loop = g_main_loop_new(NULL, FALSE);

	/* FIXME: Use a better mechanism to select dynamically protos */
	if (strcmp(opt_proto, "http") == 0)
		http_register();
	else
		ws_register();

	err = manager_start();
	if (err < 0) {
		printf("start(): %s (%d)\n", strerror(-err), -err);
		goto done;
	}

	g_main_loop_run(main_loop);

	g_main_loop_unref(main_loop);

	if (strcmp(opt_proto, "http") == 0)
		http_unregister();
	else
		ws_unregister();

	manager_stop();

	printf("Exiting\n");

	return err;

done:
	return err;
}
