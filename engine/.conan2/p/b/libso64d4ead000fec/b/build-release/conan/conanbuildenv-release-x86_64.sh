script_folder="/home/lcw/quantitative_trading_projects/engine/.conan2/p/b/libso64d4ead000fec/b/build-release/conan"
echo "echo Restoring environment" > "$script_folder/deactivate_conanbuildenv-release-x86_64.sh"
for v in 
do
   is_defined="true"
   value=$(printenv $v) || is_defined="" || true
   if [ -n "$value" ] || [ -n "$is_defined" ]
   then
       echo export "$v='$value'" >> "$script_folder/deactivate_conanbuildenv-release-x86_64.sh"
   else
       echo unset $v >> "$script_folder/deactivate_conanbuildenv-release-x86_64.sh"
   fi
done
