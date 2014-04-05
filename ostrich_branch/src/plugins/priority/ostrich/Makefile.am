# Makefile for priority/ostrich plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = priority_ostrich.la

# OStrich priority logging plugin.
priority_ostrich_la_SOURCES = priority_ostrich.c
priority_ostrich_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
