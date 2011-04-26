##############################################################################
#
# Keyspace Makefile
#
##############################################################################

all: release

##############################################################################
#
# Directories
#
##############################################################################

BASE_DIR = .
KEYSPACE_DIR = .
BIN_DIR = $(BASE_DIR)/bin

VERSION = `$(BASE_DIR)/script/version.sh 3 $(SRC_DIR)/Version.h`
VERSION_MAJOR = `$(BASE_DIR)/script/version.sh 1 $(SRC_DIR)/Version.h`
VERSION_MAJMIN = `$(BASE_DIR)/script/version.sh 3 $(SRC_DIR)/Version.h`
PACKAGE_NAME = scaliendb-server
PACKAGE_DIR = $(BUILD_DIR)/packages
PACKAGE_FILE = $(PACKAGE_NAME)-$(VERSION).deb
PACKAGE_REPOSITORY = /var/www/debian.scalien.com/

BUILD_ROOT = $(BASE_DIR)/build
SRC_DIR = $(BASE_DIR)/src
SCRIPT_DIR = $(BASE_DIR)/script
DEB_DIR = $(BUILD_ROOT)/deb
DIST_DIR = $(BUILD_ROOT)/dist

BUILD_DEBUG_DIR = $(BUILD_ROOT)/Debug
BUILD_RELEASE_DIR = $(BUILD_ROOT)/Release

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

OBJECTS = \
	$(ALL_OBJECTS) \
	$(BUILD_DIR)/Main.o

TEST_OBJECTS = \
	$(BUILD_DIR)/Test/ClientTest.o \
	$(BUILD_DIR)/Test/Test.o

SWIG_WRAPPER_OBJECT = \
	$(BUILD_DIR)/Application/Client/SDBPClientWrapper.o

CLIENT_DIR = \
	Application/Client

CLIENT_WRAPPER_FILES = \
	$(SRC_DIR)/$(CLIENT_DIR)/scaliendb_client.i \
	$(SRC_DIR)/$(CLIENT_DIR)/SDBPClientWrapper.h \
	$(SRC_DIR)/$(CLIENT_DIR)/SDBPClientWrapper.cpp
	
CLIENTLIBS = \
	$(BIN_DIR)/$(ALIB) \
	$(BIN_DIR)/$(SOLIB)


EXECUTABLES = \
	$(BUILD_DIR)/ScalienDB \
	$(BUILD_DIR)/Test \
	$(BIN_DIR)/scaliendb
	
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -o $@ -c $<

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $<

$(BIN_DIR)/%.so: $(BIN_DIR)/%.a
	$(CC) $(SOLINK) -o $@ $< $(SOLIBS)


# libraries
$(BIN_DIR)/$(ALIB): $(BUILD_DIR) $(CLIENTLIB_OBJECTS)
	$(AR) -r $@ $(CLIENTLIB_OBJECTS)
	$(RANLIB) $@

$(BIN_DIR)/$(SOLIB): $(BUILD_DIR) $(CLIENTLIB_OBJECTS)
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
	$(CXX) $(SWIG_LDFLAGS) -o $@ $(BUILD_DIR)/$(PYTHON_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB)
	-cp -rf $(SRC_DIR)/$(PYTHON_CLIENT_DIR)/scaliendb.py $(BIN_DIR)/$(PYTHON_DIR)
	-cp -rf $(SRC_DIR)/$(PYTHON_CLIENT_DIR)/scaliendb_client.py $(BIN_DIR)/$(PYTHON_DIR)

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
	-I/usr/lib/jvm/java-6-sun/include/linux

JAVA_SOURCE_FILES = \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Client.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/SDBPException.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/ListParams.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Result.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Status.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Database.java \
	$(SRC_DIR)/$(JAVA_CLIENT_DIR)/Table.java

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
	$(CXX) $(SWIG_LDFLAGS) -o $@ $(BUILD_DIR)/$(JAVA_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB)

$(BIN_DIR)/$(JAVA_DIR)/$(JAVA_JAR_FILE): $(SRC_DIR)/$(JAVA_CLIENT_WRAPPER).cpp $(JAVA_SOURCE_FILES)
	-mkdir -p $(BIN_DIR)/$(JAVA_DIR)/$(JAVA_PACKAGE_DIR)
	cp -rf $(SRC_DIR)/$(JAVA_CLIENT_DIR)/*.java $(BIN_DIR)/$(JAVA_DIR)/$(JAVA_PACKAGE_DIR)
	cd $(BIN_DIR)/$(JAVA_DIR) && javac $(JAVA_PACKAGE_DIR)/Client.java && jar cf $(JAVA_JAR_FILE) $(JAVA_PACKAGE_DIR)/*.class && rm -rf com
	

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

$(BIN_DIR)/$(PHP_DIR)/$(PHP_LIB): $(BIN_DIR)/$(ALIB) $(SWIG_WRAPPER_OBJECT) $(BUILD_DIR)/$(PHP_CLIENT_WRAPPER).o
	-mkdir -p $(BIN_DIR)/$(PHP_DIR)
	$(CXX) $(SWIG_LDFLAGS) -o $@ $(BUILD_DIR)/$(PHP_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB)
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
	$(CXX) $(SWIG_LDFLAGS) -o $@ $(BUILD_DIR)/$(RUBY_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB)
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
	$(CXX) $(SWIG_LDFLAGS) -o $@ $(BUILD_DIR)/$(PERL_CLIENT_WRAPPER).o $(SWIG_WRAPPER_OBJECT) $(BIN_DIR)/$(ALIB)
	-cp -rf $(SRC_DIR)/$(PERL_CLIENT_DIR)/scaliendb.pm $(BIN_DIR)/$(PERL_DIR)
	-cp -rf $(SRC_DIR)/$(PERL_CLIENT_DIR)/scaliendb_client.pm $(BIN_DIR)/$(PERL_DIR)

# executables	
$(BUILD_DIR)/ScalienDB: $(BUILD_DIR) $(LIBS) $(OBJECTS)
	$(CXX) $(LDFLAGS) -o $@ $(OBJECTS) $(LIBS)

$(BIN_DIR)/clienttest: $(BUILD_DIR) $(TEST_OBJECTS) $(BIN_DIR)/$(ALIB)
	$(CXX) $(LDFLAGS) -o $@ $(TEST_OBJECTS) $(LIBS) $(BIN_DIR)/$(ALIB)

$(BIN_DIR)/bdbtool: $(BUILD_DIR) $(LIBS) $(ALL_OBJECTS) $(BUILD_DIR)/Application/Tools/BDBTool/BDBTool.o
	$(CXX) $(LDFLAGS) -o $@ $(ALL_OBJECTS) $(LIBS) $(BUILD_DIR)/Application/Tools/BDBTool/BDBTool.o

$(BUILD_DIR)/TestMain: $(BUILD_DIR) $(TEST_OBJECTS) $(ALL_OBJECTS) $(BUILD_DIR)/Test/TestMain.o $(BUILD_DIR)/Test/Test.o $(CLIENTLIB_OBJECTS)
	$(CXX) $(LDFLAGS) $(CFLAGS) -o $@ $(TEST_OBJECTS) $(ALL_OBJECTS) $(BUILD_DIR)/Test/TestMain.o $(BUILD_DIR)/Test/Test.o $(LIBS) $(CLIENTLIB_OBJECTS)

$(BUILD_DIR)/Test/TestMain.o: $(SRC_DIR)/Test/TestMain.cpp
	$(CXX) $(CFLAGS) -o $@ -DTEST -c $(SRC_DIR)/Test/TestMain.cpp

$(BIN_DIR)/scaliendb: $(BUILD_DIR)/ScalienDB
	-cp -fr $< $@
	-cp -fr $(SCRIPT_DIR)/cluster-exec.sh $(BIN_DIR)
	-cp -fr $(SCRIPT_DIR)/scaliendb-env.sh $(BIN_DIR)
	-cp -rf $(SCRIPT_DIR)/safe_scaliendb $(BIN_DIR)

$(BIN_DIR)/cli: pythonlib $(SCRIPT_DIR)/cli $(SCRIPT_DIR)/shell.py
	-cp -fr $(SCRIPT_DIR)/scaliendb-env.sh $(BIN_DIR)
	-cp -fr $(SCRIPT_DIR)/shell.py $(BIN_DIR)
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

clientlib:
	$(MAKE) clientlibs BUILD="debug"

#pythonlib: $(BUILD_DIR) $(CLIENTLIBS) $(PYTHONLIB)
pythonlib: $(BUILD_DIR) clientlib
	$(MAKE) $(PYTHONLIB) BUILD="debug"

javalib: $(BUILD_DIR) $(CLIENTLIBS) $(JAVALIB)

phplib: $(BUILD_DIR) $(CLIENTLIBS) $(PHPLIB)

rubylib: $(BUILD_DIR) $(CLIENTLIBS) $(RUBYLIB)

perllib: $(BUILD_DIR) $(CLIENTLIBS) $(PERLLIB)

#targets: $(BUILD_DIR) executables clientlibs
targets: $(BUILD_DIR) executables

testmain: $(BUILD_DIR) $(BUILD_DIR)/TestMain

clientlibs: $(BUILD_DIR) $(CLIENTLIBS)

executables: $(BUILD_DIR) $(EXECUTABLES)

cli: $(BUILD_DIR) $(BIN_DIR)/cli

javadoc: $(JAVA_SOURCE_FILES) 
	-SRCDIR=`pwd`; cd $(SRC_DIR)/$(JAVA_CLIENT_DIR) && javadoc -d $$SRCDIR/javadoc -public *.java

install: release
	-cp -fr $(BIN_DIR)/$(ALIB) $(INSTALL_LIB_DIR)
	-cp -fr $(BIN_DIR)/$(SOLIB) $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION)
	-ln -sf $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION) $(INSTALL_LIB_DIR)/$(LIBNAME).$(SOEXT).$(VERSION_MAJOR)
	-ln -sf $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION) $(INSTALL_LIB_DIR)/$(LIBNAME).$(SOEXT)
	-cp -fr $(SRC_DIR)/Application/ScalienDB/Client/scaliendb_client.h $(INSTALL_INCLUDE_DIR)
	-cp -fr $(BIN_DIR)/scaliend $(INSTALL_BIN_DIR)
	-cp -fr $(SCRIPT_DIR)/safe_scaliend $(INSTALL_BIN_DIR)

uninstall:
	-rm $(INSTALL_LIB_DIR)/$(ALIB)
	-rm $(INSTALL_LIB_DIR)/$(SOLIB).$(VERSION)
	-rm $(INSTALL_LIB_DIR)/$(SOLIB)
	-rm $(INSTALL_INCLUDE_DIR)/Application/ScalienDB/Client/scaliendb_client.h
	-rm $(INSTALL_BIN_DIR)/scaliendb
	-rm $(INSTALL_BIN_DIR)/safe_scaliendb

selfupdate:
	-script/updatemakefile.sh


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

clean-swig: clean-pythonlib-swig clean-javalib-swig clean-phplib-swig clean-rubylib-swig clean-perllib-swig

clean-executables:
	-rm -rf $(EXECUTABLES) 2>&1

clean-clientlib:
	-rm -f $(TEST_OBJECTS) 2>&1
	-rm -f $(BIN_DIR)/clienttest 2>&1 
	-rm -f $(BIN_DIR)/clientlib.a 2>&1

distclean: clean distclean-libs

distclean-libs: distclean-scaliendb

distclean-scaliendb:
	cd $(KEYSPACE_DIR); $(MAKE) distclean


##############################################################################
#
# Build packages
#
##############################################################################

# debian package
deb: clean release
	-mkdir -p $(DEB_DIR)/etc/init.d
	-mkdir -p $(DEB_DIR)/etc/default
	-mkdir -p $(DEB_DIR)/etc/scaliendb
	-mkdir -p $(DEB_DIR)/usr/bin
	-mkdir -p $(DEB_DIR)/DEBIAN
	-$(SCRIPT_DIR)/debian/mkcontrol.sh $(DEB_DIR)/DEBIAN/control $(PACKAGE_NAME) $(VERSION) $(shell dpkg-architecture -qDEB_BUILD_ARCH)
	-cp -fr $(SCRIPT_DIR)/debian/conffiles $(DEB_DIR)/DEBIAN
	-cp -fr $(SCRIPT_DIR)/debian/postinst $(DEB_DIR)/DEBIAN
	-cp -fr $(SCRIPT_DIR)/debian/scaliendb $(DEB_DIR)/etc/init.d
	-cp -fr $(SCRIPT_DIR)/debian/default $(DEB_DIR)/etc/default/scaliendb
	-cp -fr $(SCRIPT_DIR)/debian/shard.conf $(DEB_DIR)/etc/scaliendb
	-cp -fr $(SCRIPT_DIR)/debian/controller.conf $(DEB_DIR)/etc/scaliendb
	-cp -fr $(SCRIPT_DIR)/safe_scaliendb $(DEB_DIR)/usr/bin
	-cp -fr $(BIN_DIR)/scaliendb $(DEB_DIR)/usr/bin
	-mkdir -p $(DIST_DIR)
	-rm -f $(DIST_DIR)/$(PACKAGE_FILE)
	-cd $(DIST_DIR)
	-fakeroot dpkg -b $(DEB_DIR) $(DIST_DIR)/$(PACKAGE_FILE)

deb-install: deb
	-mkdir -p $(PACKAGE_REPOSITORY)/conf
	-cp -fr $(SCRIPT_DIR)/debian-distributions $(PACKAGE_REPOSITORY)/conf/distributions
	-reprepro -Vb $(PACKAGE_REPOSITORY) includedeb etch $(DIST_DIR)/$(PACKAGE_FILE)
