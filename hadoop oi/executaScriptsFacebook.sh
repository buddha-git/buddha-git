echo '-----------'
echo 'INICIO'
echo '-----------'
echo ''
date
echo ''

echo 'PASSO 1'
hive -f 1_stg_facebook_de_para_1.hql 

echo 'FIM'
echo '-----------'

date
