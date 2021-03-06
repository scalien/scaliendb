##############################################################################
#
# ScalienDB Makefile
#
##############################################################################

all: release

##############################################################################
#
# Directories
#
##############################################################################

BASE_DIR = .
SCALIENDB_DIR = .
BIN_DIR = $(BASE_DIR)/bin

VERSION = `$(BASE_DIR)/script/version.sh 3 $(SRC_DIR)/Version.h`
VERSION_MAJOR = `$(BASE_DIR)/script/version.sh 1 $(SRC_DIR)/Version.h`
VERSION_MAJMIN = `$(BASE_DIR)/script/version.sh 3 $(SRC_DIR)/Version.h`

BUILD_ROOT = $(BASE_DIR)/build
SRC_DIR = $(BASE_DIR)/src
SCRIPT_DIR = $(BASE_DIR)/script
DEB_DIR = $(BUILD_ROOT)/deb
DIST_DIR = $(BUILD_ROOT)/dist

BUILD_DEBUG_DIR = $(BUILD_ROOT)/mkdebug
BUILD_RELEASE_DIR = $(BUILD_ROOT)/mkrelease

INSTALL_BIN_DIR = /usr/local/bin
INSTALL_LIB_DIR = /usr/local/lib
INSTALL_INCLUDE_DIR = /usr/local/include/


##############################################################################
#
# Platform selector
#
##############################################################################
ifeq ($(shell uname),Darwin)
PLATFORM=Darwin
else
ifeq ($(shell uname),FreeBSD)
PLATFORM=Darwin
else
ifeq ($(shell uname),OpenBSD)
PLATFORM=Darwin 
else
PLATFORM=Linux
endif
endif
endif

ARCH=$(shell arch)
export PLATFORM
PLATFORM_UCASE=$(shell echo $(PLATFORM) | tr [a-z] [A-Z])

export PLATFORM_UCASE
include Makefile.$(PLATFORM)


##############################################################################
#
# Compiler/linker options
#
##############################################################################

CC = gcc
CXX = g++
RANLIB = ranlib

DEBUG_CFLAGS = -g -DDEBUG
DEBUG_LDFLAGS =

RELEASE_CFLAGS = -O2

LIBNAME = libscaliendbclient
SONAME = $(LIBNAME).$(SOEXT).$(VERSION_MAJOR)
SOLIB = $(LIBNAME).$(SOEXT)
ALIB = $(LIBNAME).a

ifeq ($(BUILD), debug)
BUILD_DIR = $(BUILD_DEBUG_DIR)
CFLAGS = $(BASE_CFLAGS) $(DEBUG_CFLAGS) $(EXTRA_CFLAGS)
CXXFLAGS = $(BASE_CXXFLAGS) $(DEBUG_CFLAGS) $(EXTRA_CFLAGS)
LDFLAGS = $(BASE_LDFLAGS) $(DEBUG_LDFLAGS)
else
BUILD_DIR = $(BUILD_RELEASE_DIR)
CFLAGS = $(BASE_CFLAGS) $(RELEASE_CFLAGS) $(EXTRA_CFLAGS)
CXXFLAGS = $(BASE_CXXFLAGS) $(RELEASE_CFLAGS) $(EXTRA_CFLAGS)
LDFLAGS = $(BASE_LDFLAGS) $(RELEASE_LDFLAGS)
endif


##############################################################################
#
# Build rules
#
##############################################################################

include Makefile.dirs
include Makefile.objects
include Makefile.clientlib
include Makefile.packages

OBJECTS = \
	$(ALL_OBJECTS) \
	$(BUILD_DIR)/Main.o

TEST_OBJECTS = \
	$(BUILD_DIR)/Test/ClientTest.o \
	$(BUILD_DIR)/Test/Test.o \


SWIG_WRAPPER_OBJECT = \
	$(BUILD_DIR)/Application/Client/SDBPClientWrapper.o

CLIENT_DIR = \
	Application/Client

TEST_DIR = \
	test

CLIENT_WRAPPER_FILES = \
	$(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i \
	$(SRC_DIR)/$(CLIENT_DIR)/SDBPClientWrapper.h \
	$(SRC_DIR)/$(CLIENT_DIR)/SDBPClientWrapper.cpp
	
CLIENTLIBS = \
	$(BIN_DIR)/$(ALIB) \
	$(BIN_DIR)/$(SOLIB)


EXECUTABLES = \
	$(BUILD_DIR)/scaliendb \
	$(BIN_DIR)/scaliendb
	
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -o $@ -c $<

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $<

$(BIN_DIR)/%.so: $(BIN_DIR)/%.a
	$(CC) $(SOLINK) -o $@ $< $(SOLIBS)


# libraries
$(BIN_DIR)/$(ALIB): $(CLIENT_BUILD_DIR) $(CLIENTLIB_OBJECTS)
	$(AR) -r $@ $(CLIENTLIB_OBJECTS)
	$(RANLIB) $@

$(BIN_DIR)/$(SOLIB): $(CLIENT_BUILD_DIR) $(CLIENTLIB_OBJECTS)
	$(CC) $(SOLINK) -o $@ $(CLIENTLIB_OBJECTS) $(LDFLAGS)

# python wrapper
PYTHON_DIR = python
PYTHON_LIB = _scaliendb_client.so
PYTHON_CONFIG = python-config

PYTHON_CLIENT_DIR = \
	$(CLIENT_DIR)/Python

PYTHON_CLIENT_WRAPPER = \
	$(PYTHON_CLIENT_DIR)/scaliendb_client_python

PYTHONLIB = \
	$(BIN_DIR)/$(PYTHON_DIR)/$(PYTHON_LIB)

$(SRC_DIR)/$(PYTHON_CLIENT_WRAPPER).cpp: $(CLIENT_WRAPPER_FILES)
	-swig -python -modern -c++ -outdir $(SRC_DIR)/$(PYTHON_CLIENT_DIR) -DSWIG_PYTHON_SAFE_CSTRINGS -o $@ -I$(SRC_DIR)/$(PYTHON_CLIENT_DIR) $(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i
	-script/fix_swig_python.sh $(SRC_DIR)/$(PYTHON_CLIENT_DIR)

$(BUILD_DIR)/$(PYTHON_CLIENT_WRAPPER).o: $(BUILD_DIR) $(SRC_DIR)/$(PYTHON_CLIENT_WRAPPER).cpp
	$(CXX) $(CXXFLAGS) `$(PYTHON_CONFIG) --includes` -o $@ -c $(SRC_DIR)/$(PYTHON_CLIENT_WRAPPER).cpp

$(BIN_DIR)/$(PYTHON_DIR)/$(PYTHON_LIB): $(BIN_DIR)/$(ALIB) $(SWIG_WRAPPER_OBJECT) $(BUILD_DIR)/$(PYTHON_CLIENT_WRAPPER).o $(SRC_DIR)/$(PYTHON_CLIENT_DIR)/scaliendb.py
	-mkdir -p $(BIN_DIR)/$(PYTHON_DIR)
	$(CXX) -o $@ $(BUILD_DIR)/$(PYTHON_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB) $(SWIG_LDFLAGS) 
	-cp -rf $(SRC_DIR)/$(PYTHON_CLIENT_DIR)/scaliendb.py $(BIN_DIR)/$(PYTHON_DIR)
	-cp -rf $(SRC_DIR)/$(PYTHON_CLIENT_DIR)/scaliendb_client.py $(BIN_DIR)/$(PYTHON_DIR)
	-cp -rf $(SRC_DIR)/$(PYTHON_CLIENT_DIR)/scaliendb_mapreduce.py $(BIN_DIR)/$(PYTHON_DIR)

# java wrapper
JAVA_DIR = java
JAVA_PACKAGE_DIR = com/scalien/scaliendb
JAVA_PACKAGE = com.scalien.scaliendb
JAVA_LIB = libscaliendb_client.$(SOEXT)
JAVA_JAR_FILE = scaliendb.jar
JAVA_INCLUDE = \
	-I/System/Library/Frameworks/JavaVM.framework/Versions/1.6.0/Headers/ \
	-I/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Headers/ \
	-I/Developer/SDKs/MacOSX10.6.sdk/System/Library/Frameworks/JavaVM.framework/Versions/1.6.0/Headers/ \
	-I/Developer/SDKs/MacOSX10.5.sdk/System/Library/Frameworks/JavaVM.framework/Versions/1.6.0/Headers/ \
	-I/Developer/SDKs/MacOSX10.5.sdk/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Headers/ \
	-I/usr/java/jdk/include/ \
	-I/usr/lib/jvm/java-6-sun/include/ \
	-I/usr/lib/jvm/java-6-sun/include/linux \
	-I/usr/lib/jvm/java-6-openjdk/include \
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/linux \
	

JAVA_SOURCE_FILES = \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Client.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/SDBPException.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Result.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Status.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Database.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Table.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Sequence.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/StringKeyIterator.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/StringKeyValueIterator.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/StringRangeParams.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/ByteKeyIterator.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/ByteKeyValueIterator.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/ByteRangeParams.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/ByteArrayComparator.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/KeyValue.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Query.java \

JAVA_CLIENT_DIR = \
	$(CLIENT_DIR)/Java

JAVA_CLIENT_WRAPPER = \
	$(JAVA_CLIENT_DIR)/scaliendb_client_java

JAVALIB = \
	$(BIN_DIR)/$(JAVA_DIR)/$(JAVA_JAR_FILE) $(BIN_DIR)/$(JAVA_DIR)/$(JAVA_LIB)

$(SRC_DIR)/$(JAVA_CLIENT_WRAPPER).cpp: $(CLIENT_WRAPPER_FILES)
	-swig -java -c++ -package $(JAVA_PACKAGE) -outdir $(SRC_DIR)/$(JAVA_CLIENT_DIR) -o $@ -I$(SRC_DIR)/$(JAVA_CLIENT_DIR) $(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i

$(BUILD_DIR)/$(JAVA_CLIENT_WRAPPER).o: $(BUILD_DIR) $(SRC_DIR)/$(JAVA_CLIENT_WRAPPER).cpp
	$(CXX) $(CXXFLAGS) $(JAVA_INCLUDE) -o $@ -c $(SRC_DIR)/$(JAVA_CLIENT_WRAPPER).cpp

$(BIN_DIR)/$(JAVA_DIR)/$(JAVA_LIB): $(BIN_DIR)/$(ALIB) $(SWIG_WRAPPER_OBJECT) $(BUILD_DIR)/$(JAVA_CLIENT_WRAPPER).o
	-mkdir -p $(BIN_DIR)/$(JAVA_DIR)
	$(CXX) -o $@ $(BUILD_DIR)/$(JAVA_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB) $(SWIG_LDFLAGS)

$(BIN_DIR)/$(JAVA_DIR)/$(JAVA_JAR_FILE): $(SRC_DIR)/$(JAVA_CLIENT_WRAPPER).cpp $(JAVA_SOURCE_FILES)
	-mkdir -p $(BIN_DIR)/$(JAVA_DIR)/$(JAVA_PACKAGE_DIR)
	cp -rf $(SRC_DIR)/$(JAVA_CLIENT_DIR)/*.java $(BIN_DIR)/$(JAVA_DIR)/$(JAVA_PACKAGE_DIR)
	cd $(BIN_DIR)/$(JAVA_DIR) && javac $(JAVA_PACKAGE_DIR)/Client.java && jar cf $(JAVA_JAR_FILE) $(JAVA_PACKAGE_DIR)/*.class && rm -rf com
	
# csharp wrapper
CSHARP_DIR = csharp
CSHARP_NAMESPACE = Scalien

CSHARPLIB=$(SRC_DIR)/$(CSHARP_CLIENT_WRAPPER).cpp

CSHARP_CLIENT_DIR = \
	$(CLIENT_DIR)/CSharp

CSHARP_CLIENT_WRAPPER = \
	$(CSHARP_CLIENT_DIR)/scaliendb_client_csharp

$(SRC_DIR)/$(CSHARP_CLIENT_WRAPPER).cpp: $(CLIENT_WRAPPER_FILES)
	-swig -csharp -c++ -namespace $(CSHARP_NAMESPACE) -outdir $(SRC_DIR)/$(CSHARP_CLIENT_DIR)/ScalienClient -o $@ -I$(SRC_DIR)/$(CSHARP_CLIENT_DIR) $(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i


# php wrapper
PHP_DIR = php
PHP_LIB = scaliendb_client.so
PHP_INCLUDE = 
PHP_CONFIG = php-config

PHP_CLIENT_DIR = $(CLIENT_DIR)/PHP
PHP_CLIENT_WRAPPER = $(PHP_CLIENT_DIR)/scaliendb_client_php
PHPLIB = $(BIN_DIR)/$(PHP_DIR)/$(PHP_LIB)

$(SRC_DIR)/$(PHP_CLIENT_WRAPPER).cpp: $(CLIENT_WRAPPER_FILES)
	-swig -php5 -c++  -outdir $(SRC_DIR)/$(PHP_CLIENT_DIR) -o $@ -I$(SRC_DIR)/$(PHP_CLIENT_DIR) $(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i
	-script/fix_swig_php.sh $(SRC_DIR)/$(PHP_CLIENT_DIR)

$(BUILD_DIR)/$(PHP_CLIENT_WRAPPER).o: $(BUILD_DIR) $(SRC_DIR)/$(PHP_CLIENT_WRAPPER).cpp
	$(CXX) $(CXXFLAGS) $(PHP_INCLUDE) `$(PHP_CONFIG) --includes` -I$(SRC_DIR)/$(PHP_CLIENT_DIR) -o $@ -c $(SRC_DIR)/$(PHP_CLIENT_WRAPPER).cpp

$(BIN_DIR)/$(PHP_DIR)/$(PHP_LIB): $(BIN_DIR)/$(ALIB) $(SWIG_WRAPPER_OBJECT) $(BUILD_DIR)/$(PHP_CLIENT_WRAPPER).o $(SRC_DIR)/$(PHP_CLIENT_DIR)/scaliendb.php
	-mkdir -p $(BIN_DIR)/$(PHP_DIR)
	$(CXX) -o $@ $(BUILD_DIR)/$(PHP_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB) $(SWIG_LDFLAGS)
	php -l $(SRC_DIR)/$(PHP_CLIENT_DIR)/scaliendb.php
	-cp -rf $(SRC_DIR)/$(PHP_CLIENT_DIR)/scaliendb.php $(BIN_DIR)/$(PHP_DIR)
	-cp -rf $(SRC_DIR)/$(PHP_CLIENT_DIR)/scaliendb_client.php $(BIN_DIR)/$(PHP_DIR)

# ruby wrapper
RUBY_DIR = ruby
RUBY_LIB = scaliendb_client.$(BUNDLEEXT)
RUBY_INCLUDE = -I/System/Library/Frameworks/Ruby.framework/Versions/1.8/Headers/ -I/usr/include/ruby-1.9.0/ruby/ruby.h

RUBY_CLIENT_DIR = $(CLIENT_DIR)/Ruby
RUBY_CLIENT_WRAPPER = $(RUBY_CLIENT_DIR)/scaliendb_client_ruby
RUBYLIB = $(BIN_DIR)/$(RUBY_DIR)/$(RUBY_LIB)

$(SRC_DIR)/$(RUBY_CLIENT_WRAPPER).cpp: $(CLIENT_WRAPPER_FILES)
	-swig -ruby -c++  -outdir $(SRC_DIR)/$(RUBY_CLIENT_DIR) -o $@ -I$(SRC_DIR)/$(RUBY_CLIENT_DIR) $(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i

$(BUILD_DIR)/$(RUBY_CLIENT_WRAPPER).o: $(BUILD_DIR) $(SRC_DIR)/$(RUBY_CLIENT_WRAPPER).cpp
	$(CXX) $(CXXFLAGS) $(RUBY_INCLUDE) -I$(SRC_DIR)/$(RUBY_CLIENT_DIR) -o $@ -c $(SRC_DIR)/$(RUBY_CLIENT_WRAPPER).cpp

$(BIN_DIR)/$(RUBY_DIR)/$(RUBY_LIB): $(BIN_DIR)/$(ALIB) $(SWIG_WRAPPER_OBJECT) $(BUILD_DIR)/$(RUBY_CLIENT_WRAPPER).o
	-mkdir -p $(BIN_DIR)/$(RUBY_DIR)
	$(CXX) -o $@ $(BUILD_DIR)/$(RUBY_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB) $(SWIG_LDFLAGS)
	-cp -rf $(SRC_DIR)/$(RUBY_CLIENT_DIR)/scaliendb.rb $(BIN_DIR)/$(RUBY_DIR)

# perl wrapper
PERL_DIR = perl
PERL_LIB = scaliendb_client.$(BUNDLEEXT)
PERL_INCLUDE = -I/opt/local/lib/perl5/5.8.9/darwin-2level/CORE/ -I/usr/lib/perl/5.10/CORE/

PERL_CLIENT_DIR = $(CLIENT_DIR)/Perl
PERL_CLIENT_WRAPPER = $(PERL_CLIENT_DIR)/scaliendb_client_perl
PERLLIB = $(BIN_DIR)/$(PERL_DIR)/$(PERL_LIB)

$(SRC_DIR)/$(PERL_CLIENT_WRAPPER).cpp: $(CLIENT_WRAPPER_FILES)
	-swig -perl -c++  -outdir $(SRC_DIR)/$(PERL_CLIENT_DIR) -o $@ -I$(SRC_DIR)/$(PERL_CLIENT_DIR) $(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i

$(BUILD_DIR)/$(PERL_CLIENT_WRAPPER).o: $(BUILD_DIR) $(SRC_DIR)/$(PERL_CLIENT_WRAPPER).cpp
	$(CXX) $(CXXFLAGS) $(PERL_INCLUDE) -I$(SRC_DIR)/$(PERL_CLIENT_DIR) -o $@ -c $(SRC_DIR)/$(PERL_CLIENT_WRAPPER).cpp

$(BIN_DIR)/$(PERL_DIR)/$(PERL_LIB): $(BIN_DIR)/$(ALIB) $(SWIG_WRAPPER_OBJECT) $(BUILD_DIR)/$(PERL_CLIENT_WRAPPER).o
	-mkdir -p $(BIN_DIR)/$(PERL_DIR)
	$(CXX) -o $@ $(BUILD_DIR)/$(PERL_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB) $(SWIG_LDFLAGS)
	-cp -rf $(SRC_DIR)/$(PERL_CLIENT_DIR)/scaliendb.pm $(BIN_DIR)/$(PERL_DIR)
	-cp -rf $(SRC_DIR)/$(PERL_CLIENT_DIR)/scaliendb_client.pm $(BIN_DIR)/$(PERL_DIR)

# executables	
$(BUILD_DIR)/scaliendb: $(BUILD_DIR) $(LIBS) $(OBJECTS)
	$(CXX) -o $@ $(OBJECTS) $(LIBS) $(LDFLAGS)

$(BIN_DIR)/clienttest: $(BUILD_DIR) $(TEST_OBJECTS) $(BIN_DIR)/$(ALIB)
	$(CXX) -o $@ $(TEST_OBJECTS) $(LIBS) $(BIN_DIR)/$(ALIB) $(LDFLAGS)

$(BIN_DIR)/bdbtool: $(BUILD_DIR) $(LIBS) $(ALL_OBJECTS) $(BUILD_DIR)/Application/Tools/BDBTool/BDBTool.o
	$(CXX)-o $@ $(ALL_OBJECTS) $(LIBS)  $(LDFLAGS) $(BUILD_DIR)/Application/Tools/BDBTool/BDBTool.o

$(BUILD_DIR)/TestMain: $(BUILD_DIR) $(TEST_OBJECTS) $(ALL_OBJECTS) $(BUILD_DIR)/Test/TestMain.o $(BUILD_DIR)/Test/Test.o $(CLIENTLIB_OBJECTS)
	$(CXX) $(CFLAGS) -o $@ $(TEST_OBJECTS) $(ALL_OBJECTS) $(BUILD_DIR)/Test/TestMain.o $(BUILD_DIR)/Test/Test.o $(LIBS) $(LDFLAGS) $(CLIENTLIB_OBJECTS)

$(BUILD_DIR)/Test/TestMain.o: $(SRC_DIR)/Test/TestMain.cpp
	$(CXX) $(CFLAGS) -o $@ -DTEST -c $(SRC_DIR)/Test/TestMain.cpp

$(BIN_DIR)/scaliendb: $(BUILD_DIR)/scaliendb
	-cp -fr $< $@
	-cp -fr $(SCRIPT_DIR)/cluster-exec.sh $(BIN_DIR)
	-cp -fr $(SCRIPT_DIR)/scaliendb-env.sh $(BIN_DIR)
	-cp -rf $(SCRIPT_DIR)/safe_scaliendb $(BIN_DIR)

$(BIN_DIR)/cli: pythonlib $(SCRIPT_DIR)/cli $(SCRIPT_DIR)/shell.py
	-cp -fr $(SCRIPT_DIR)/scaliendb-env.sh $(BIN_DIR)
	-rm -f $(BIN_DIR)/shell.py
	-$(SCRIPT_DIR)/fix_python.sh $(SCRIPT_DIR)/shell.py > $(BIN_DIR)/shell.py
	-cp -fr $(SCRIPT_DIR)/cli $(BIN_DIR)


##############################################################################
#
# Targets
#
##############################################################################

debug:
	$(MAKE) targets BUILD="debug"

release:
	$(MAKE) targets BUILD="release"

check: clean
	$(MAKE) targets EXTRA_CFLAGS=-Werror

tester:
	$(MAKE) testmain BUILD="debug"

testobjs:
	$(MAKE) objects BUILD="debug"

objects: $(BUILD_DIR) $(ALL_OBJECTS) $(TEST_OBJECTS) $(CLIENTLIB_OBJECTS)

clienttest:
	$(MAKE) targets BUILD="release"

clientlib-target: scversion
	$(MAKE) $(CLIENTLIB_TARGET) BUILD_DIR=$(BUILD_DIR)/client EXTRA_CFLAGS="-DCLIENT_MULTITHREADED -DIOPROCESSOR_MULTITHREADED -DEVENTLOOP_MULTITHREADED"

clientlib:
	$(MAKE) clientlib-target CLIENTLIB_TARGET=clientlibs

clientlib-debug:
	$(MAKE) clientlib BUILD="debug"

pythonlib: $(BUILD_DIR) clientlib
	$(MAKE) clientlib-target CLIENTLIB_TARGET="$(PYTHONLIB)"

pythonlib-testdeploy: pythonlib
	cp $(BIN_DIR)/$(PYTHON_DIR)/* $(TEST_DIR)/$(PYTHON_DIR)

javalib: $(BUILD_DIR) clientlib
	$(MAKE) clientlib-target CLIENTLIB_TARGET="$(JAVALIB)"

javalib-testdeploy: javalib
	cp $(BIN_DIR)/$(JAVA_DIR)/* $(TEST_DIR)/$(JAVA_DIR)

csharplib: $(BUILD_DIR) $(CSHARPLIB)

phplib: $(BUILD_DIR) clientlib
	$(MAKE) clientlib-target CLIENTLIB_TARGET="$(PHPLIB)"

testlib: $(BUILD_DIR)
	$(MAKE) clientlib-target CLIENTLIB_TARGET="objects"

targets: $(BUILD_DIR) scversion executables

testmain: $(BUILD_DIR) $(BUILD_DIR)/TestMain

clientlibs: $(BUILD_DIR) $(CLIENTLIBS)

executables: $(BUILD_DIR) $(EXECUTABLES)

cli: $(BUILD_DIR) $(BIN_DIR)/cli

javadoc: $(JAVA_SOURCE_FILES) 
	-javadoc -d javadoc -public $(JAVA_SOURCE_FILES)

install: release clientlib
	-cp -fr $(BIN_DIR)/$(ALIB) $(INSTALL_LIB_DIR)
	-cp -fr $(BIN_DIR)/$(SOLIB) $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION)
	-ln -sf $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION) $(INSTALL_LIB_DIR)/$(LIBNAME).$(SOEXT).$(VERSION_MAJOR)
	-ln -sf $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION) $(INSTALL_LIB_DIR)/$(LIBNAME).$(SOEXT)
	-cp -fr $(BIN_DIR)/scaliendb $(INSTALL_BIN_DIR)
	-cp -fr $(SCRIPT_DIR)/safe_scaliendb $(INSTALL_BIN_DIR)

uninstall:
	-rm $(INSTALL_LIB_DIR)/$(ALIB)
	-rm $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION)
	-rm $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION_MAJOR)
	-rm $(INSTALL_LIB_DIR)/$(SOLIB)
	-rm $(INSTALL_BIN_DIR)/scaliendb
	-rm $(INSTALL_BIN_DIR)/safe_scaliendb

binrelease: release javalib pythonlib
	

selfupdate:
	-script/updatemakefile.sh

scversion:
	-script/createscversion.sh

ziprelease: binrelease
	-script/ziprelease.sh


##############################################################################
#
# Clean
#
##############################################################################

clean: clean-debug clean-release clean-libs clean-executables
	-rm -rf $(BUILD_ROOT) 2>&1
	-rm -rf $(DEB_DIR) 2>&1

clean-debug:
	-rm -r -f $(BUILD_DEBUG_DIR) 2>&1
	
clean-release:
	-rm -r -f $(BUILD_RELEASE_DIR) 2>&1
	
clean-libs: clean-pythonlib clean-phplib clean-javalib clean-rubylib clean-perllib
	-rm -f $(CLIENTLIBS) 2>&1

clean-pythonlib:
	-rm -f $(BIN_DIR)/$(PYTHON_DIR)/* 2>&1

clean-javalib:
	-rm -f $(BUILD_DIR)/$(JAVA_CLIENT_DIR)/* 2>&1
	-rm -rf $(BIN_DIR)/$(JAVA_DIR)/* 2>&1

clean-phplib:
	-rm -f $(BUILD_DIR)/$(PHP_CLIENT_DIR)/* 2>&1
	-rm -f $(BIN_DIR)/$(PHP_DIR)/* 2>&1

clean-rubylib:
	-rm -f $(BUILD_DIR)/$(RUBY_CLIENT_DIR)/* 2>&1
	-rm -f $(BIN_DIR)/$(RUBY_DIR)/* 2>&1

clean-perllib:
	-rm -f $(BUILD_DIR)/$(PERL_CLIENT_DIR)/* 2>&1
	-rm -f $(BIN_DIR)/$(PERL_DIR)/* 2>&1

clean-csharplib-swig:
	-rm -f $(SRC_DIR)/$(CSHARP_CLIENT_WRAPPER).cpp 2>&1

clean-pythonlib-swig:
	-rm -f $(SRC_DIR)/$(PYTHON_CLIENT_WRAPPER).cpp 2>&1

clean-javalib-swig:
	-rm -f $(SRC_DIR)/$(JAVA_CLIENT_WRAPPER).cpp 2>&1

clean-phplib-swig:
	-rm -f $(SRC_DIR)/$(PHP_CLIENT_WRAPPER).cpp 2>&1

clean-rubylib-swig:
	-rm -f $(SRC_DIR)/$(RUBY_CLIENT_WRAPPER).cpp 2>&1 

clean-perllib-swig:
	-rm -f $(SRC_DIR)/$(PERL_CLIENT_WRAPPER).cpp 2>&1

clean-swig: clean-pythonlib-swig clean-javalib-swig clean-phplib-swig clean-rubylib-swig clean-perllib-swig clean-csharplib-swig

clean-executables:
	-rm -rf $(EXECUTABLES) 2>&1

clean-clientlib:
	-rm -f $(TEST_OBJECTS) 2>&1
	-rm -f $(BIN_DIR)/clienttest 2>&1 
	-rm -f $(BIN_DIR)/clientlib.a 2>&1

distclean: clean distclean-libs

distclean-libs: distclean-scaliendb

distclean-scaliendb:
	cd $(SCALIENDB_DIR); $(MAKE) distclean
