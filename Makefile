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
# REBAR=$(shell which rebar)
REBAR=./rebar
ifeq ($(REBAR),)
$(error "Rebar not available on this system")
endif

.PHONY: all compile clean dialyze typer distclean \
  rebuild test help

all: compile dialyze

# =============================================================================
# Rules to build the system
# =============================================================================

compile:
	- $(REBAR) skip_deps=true compile

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

test:
	$(REBAR) ct

clean:
	- $(REBAR) skip_deps=true clean

distclean: clean
	- rm -rf $(DEPS_PLT)
	- rm -rvf deps
	- rm -rvf _rel

rebuild: distclean deps compile dialyze

help:
	@echo "usage examples:"
	@echo " using custom overlay vars:"
	@echo "		OVERLAY_CONFIG=config/overlays/live/vars.config make tar"
	@echo " or predefined rules:"
	@echo "		make {environment}/{platform} (eg. make staging/mobile, dev/reward-callback)"
	@echo ""
