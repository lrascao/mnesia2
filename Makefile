# Copyright 2012 Erlware, LLC. All Rights Reserved.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

DEPS_PLT=./.deps_plt
DEPS=erts kernel stdlib

# =============================================================================
# Verify that the programs we need to run are installed on this system
# =============================================================================
REBAR=$(shell which rebar)
# REBAR=./rebar
ifeq ($(REBAR),)
$(error "Rebar not available on this system")
endif

.PHONY: all compile clean dialyze typer distclean \
  rebuild test help

all: compile dialyze

travis: dialyze test

# =============================================================================
# Rules to build the system
# =============================================================================

compile:
	- $(REBAR) compile
	pushd examples; erlc *.erl; popd

debug:
	DEBUG_BUILD=true $(REBAR) compile

$(DEPS_PLT):
	@echo Building $(DEPS_PLT)
	- dialyzer --build_plt \
	   --output_plt $(DEPS_PLT) \
	   --apps $(DEPS)

dialyze: $(DEPS_PLT) compile
	- dialyzer --fullpath \
		-Werror_handling \
		-Wrace_conditions \
		-Wunderspecs \
		--plt $(DEPS_PLT) \
		ebin

typer:
	typer --plt $(DEPS_PLT) \
		  -pa deps/erlcloud \
		  -r ./src

test: clean debug
	@rm -rf ${CT_LOG}
	@find src -type f -name *.erl -exec cp {} ebin \;
	@find test -type f -name *.erl -exec cp {} ebin \;
	$(REBAR) ct
	@find ebin -type f -name "*.erl" -exec rm {} \;

clean:
	- $(REBAR) skip_deps=true clean

distclean: clean
	- rm -rf $(DEPS_PLT)
	- rm -rvf ebin
	- rm -rvf .rebar

rebuild: distclean deps compile dialyze
