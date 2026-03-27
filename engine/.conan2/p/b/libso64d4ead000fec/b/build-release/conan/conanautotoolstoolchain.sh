script_folder="/home/lcw/quantitative_trading_projects/engine/.conan2/p/b/libso64d4ead000fec/b/build-release/conan"
echo "echo Restoring environment" > "$script_folder/deactivate_conanautotoolstoolchain.sh"
for v in CPPFLAGS CXXFLAGS CFLAGS LDFLAGS PKG_CONFIG_PATH
do
   is_defined="true"
   value=$(printenv $v) || is_defined="" || true
   if [ -n "$value" ] || [ -n "$is_defined" ]
   then
       echo export "$v='$value'" >> "$script_folder/deactivate_conanautotoolstoolchain.sh"
   else
       echo unset $v >> "$script_folder/deactivate_conanautotoolstoolchain.sh"
   fi
done

export CPPFLAGS="${CPPFLAGS:-}${CPPFLAGS:+ }-DNDEBUG"
export CXXFLAGS="${CXXFLAGS:-}${CXXFLAGS:+ }-m64 -fPIC -O3"
export CFLAGS="${CFLAGS:-}${CFLAGS:+ }-m64 -fPIC -O3"
export LDFLAGS="${LDFLAGS:-}${LDFLAGS:+ }-m64"
export PKG_CONFIG_PATH="$script_folder/../../build-release/conan${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"