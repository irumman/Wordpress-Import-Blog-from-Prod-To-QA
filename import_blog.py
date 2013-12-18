#!/usr/bin/python -tt

import sys
import commands
import paramiko
import os
import re
import shutil
import MySQLdb as mdb
from optparse import OptionParser

parameter_file_path='import_blog.conf'

source_server='a'
source_os_user=''
source_os_password=''
source_db_backup_root_dir=''
source_dbbackupdir=''
source_domain=''

source_mysql_user=''
source_mysql_passwd=''
source_mysql_port=''
source_mysql_db=''


import_blog_dir=''

target_server=''
target_mysql_user=''
target_mysql_password=''
target_mysql_port=''
target_mysql_db=''
ineteractive_mode=False

hyperdb_factor = 1

target_domain=''


remove_if_exists=0
create_conf_file=''

number_of_deps=100000000

debug_mode=False

def open_ssh_connection():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=source_server, username=source_os_user, password=source_os_password)
    return ssh
    
def close_ssh_connection(ssh):
  ssh.close()
    
def find_backup_dir(ssh):    
  stdin, stdout, stderr = ssh.exec_command('cd /BACKUP; ls')
  cmd_out = stdout.readlines()
  cmd_out= ','.join(cmd_out)
  backup_dirs = re.findall(r'wordpress_\w+', cmd_out)
  backup_dirs.sort()
  backup_dir = backup_dirs[len(backup_dirs)-2]
  backup_dir=source_dbbackupdir + '/' + backup_dir
  #print backup_dir
  return backup_dir


def find_backup_file_for_blogs(blog_id_list,ssh,source_dbbackupdir,hyperdb_factor):
  #return blog_backup_path_dict
  blog_backup_path_dict = {}
  for blog_id in blog_id_list:
    if blog_id in blog_backup_path_dict:
      continue
    else:  
      blog_backup_path_dict[blog_id] = find_backup_file_for_a_blog(blog_id,ssh,source_dbbackupdir,hyperdb_factor)
  return blog_backup_path_dict


def find_backup_file_for_a_blog(blog_id,ssh,source_dbbackupdir,hyperdb_factor):
  v = int(blog_id)/hyperdb_factor
  cmd = 'test -d ' + source_dbbackupdir + '/wordpress' + str(v) + '; echo $?'
  #print cmd
  stdin, stdout, stderr = ssh.exec_command(cmd)
  cmd_out = stdout.readlines()
  cmd_out = ','.join(cmd_out)
  #print cmd_out
  if cmd_out[0] == '0':
    source_dbbackupdir = source_dbbackupdir + '/wordpress'+str(v)
  else:
    source_dbbackupdir = source_dbbackupdir + '/wordpress_old'
    
  cmd = 'ls '+ source_dbbackupdir + '/blog_' + blog_id + '.sql.*' 
  #print cmd
  stdin, stdout, stderr = ssh.exec_command(cmd)
  err = stderr.readlines()
  err = ','.join(err)
  if len(err) > 0:
    print '!!!ERROR!!! '+ err[4:]
    return 
    
  cmd_out = stdout.readlines()
  cmd_out = ','.join(cmd_out)
  #print cmd_out
  source_dbbackupdir = cmd_out.strip() 
  return source_dbbackupdir

def remove_wrong_blog_id_from_list(blog_backup_path_dict):
  tmp_dict = { k : v for k,v in blog_backup_path_dict.iteritems() if v }    
  wrong_blog_id_list = { k for k,v in blog_backup_path_dict.iteritems() if not v }    
  if len(wrong_blog_id_list) > 0:
    print "Wrong Blog IDs"
    for l in wrong_blog_id_list:
      print l
  return tmp_dict 



def display_dict(blog_backup_path_dict):
  print "Blog_id = Backup Path"
  for blog_id,path in blog_backup_path_dict.iteritems():
    print "%s = %s" %(blog_id,blog_backup_path_dict[blog_id])
         

def make_tar_file_from_blog_list(ssh,blog_backup_path_dict):
  #print ' ----- make_tar_file_from_blog_list'
  cmd = 'tar -zcf ' + '/tmp/tmp_import_blogs.tar.gz '
  for blog_id, path in   blog_backup_path_dict.iteritems():
    if path: 
      cmd = cmd + ' ' + path
  
  #print cmd
  stdin, stdout, stderr = ssh.exec_command(cmd)
  cmd_out = stdout.readlines()
  cmd_out= ','.join(cmd_out)
  #print cmd_out
  return '/tmp/tmp_import_blogs.tar.gz'



def copy_file_from_remote_machine(ssh,remote_filepath):
  #print '----copy_file_from_remote_machine----'
  sftp=ssh.open_sftp()
  remote_file = sftp.get(remotepath=remote_filepath,localpath=remote_filepath)
  sftp.close()
  return remote_filepath


def extract_import_blogs_gz(local_blog_gz):
  if os.path.exists('/tmp/import_blogs') == True:
  	shutil.rmtree('/tmp/import_blogs')
  	
  os.mkdir('/tmp/import_blogs')
  cmd = 'tar -zxf ' + local_blog_gz + ' -C ' + import_blog_dir
  os.system(cmd)
  os.chdir(import_blog_dir)
  cmd =  'find ' + import_blog_dir +'  -type f -exec mv -i {} . \;'
  os.system(cmd)
  shutil.rmtree(import_blog_dir + '/BACKUP')
  return import_blog_dir


def find_target_domain():
  tmp_out = '/tmp/blog_master_domain.out'
  cmd = 'mysql -h ' + target_server + ' -P' + target_mysql_port + ' -u' + target_mysql_user + ' --password=' +  target_mysql_password + ' -e "select domain from wordpress.wp_blogs where blog_id = 1" -N -s >' + tmp_out
  os.system(cmd)
  f = open(tmp_out,'rU')
  target_domain = f.read()
  f.close()
  os.remove(tmp_out)
  target_domain = target_domain.strip()
  return target_domain
  
def make_dumpfiles_for_target_domain(import_blog_dir,target_domain,source_domain):
  dump_files =  os.listdir(import_blog_dir)
  os.chdir(import_blog_dir)
  for f in dump_files:
   cmd = "sed  -i 's/" + source_domain + "/" + target_domain + "/g' " + f
   #print cmd 
   os.system(cmd)
   cmd = "sed  -i 's/photos." + target_domain + "/photos." + source_domain + "/g' " + f
   #print cmd 
   os.system(cmd)
   cmd = "sed  -i 's/videos." + target_domain + "/videos." + source_domain + "/g' " + f
   #print cmd 
   os.system(cmd)
   cmd = "sed  -i 's/media." + target_domain + "/media." + source_domain + "/g' " + f
   #print cmd 
   os.system(cmd)
   return import_blog_dir

def find_database_num(blog_dump_file,hyperdb_factor):
  #print blog_dump_file
  dbnum = blog_dump_file[5:blog_dump_file.find('.')]
  dbnum = int(dbnum)/hyperdb_factor
  dbname='wordpress'+str(dbnum)
  return dbname

def find_last_blog_clustered(target_mysql_con):
  cur = target_mysql_con.cursor()
  qry= "select config_value from admin_data.ds_config where config_key = 'last_blog_clustered'"
  cur.execute(qry)  
  last_blog_clustered = cur.fetchone()
  debug_log( 'Last Blog Cluster ID = ' + last_blog_clustered[0])
  return last_blog_clustered[0]

def debug_log(log_str):
  if debug_mode:
    print log_str
    
def  upload_to_mysqldb(import_blog_dir,hyperdb_factor):
  os.chdir(import_blog_dir)
  dump_files =  os.listdir(import_blog_dir)
  
  target_mysql_con = mdb.connect(host=target_server, user=target_mysql_user,passwd=target_mysql_password,db="wordpress",port=int(target_mysql_port))
  source_mysql_con = mdb.connect(host=source_server, user=source_mysql_user,passwd=source_mysql_passwd,db=source_mysql_db,port=int(source_mysql_port))
  for f in dump_files:
    
    blog_id = f[5:f.find('.')]
    print "--> Uploading blog " + str(blog_id) + " ..."
    row = check_wp_blogs(blog_id,target_mysql_con, source_mysql_con)
    
    v_type_of_entity = row["type_of_entity"]
    debug_log('Entity Type = ' + str(v_type_of_entity))
    
    if int(v_type_of_entity) == 8:
      debug_log("check_and_import_admin_team with blog_id =" + blog_id)
      check_and_import_admin_team(blog_id, target_mysql_con, source_mysql_con)
    elif int(v_type_of_entity) == 3 or int(v_type_of_entity) == 9:
      debug_log("check_and_import_admin_school_league_region with blog_id =" + blog_id)
      check_and_import_admin_school_league_region(blog_id, v_type_of_entity, target_mysql_con, source_mysql_con)
    
    last_blog_clustered = find_last_blog_clustered(target_mysql_con)
    
    if int(blog_id) <= int(last_blog_clustered):
      dbname = find_database_num(f,hyperdb_factor)
    else:
      dbname = 'wordpress_old'  
    debug_log('Target Database = ' + dbname )
    cmd = 'mysql -h ' + target_server + ' -P' + target_mysql_port + ' -u' + target_mysql_user + ' --password=' +  target_mysql_password + ' -e "CREATE DATABASE IF NOT EXISTS ' + dbname + '"'
    os.system(cmd)
    global target_mysql_db
    target_mysql_db = dbname
    remove_old_tables(blog_id)
    cmd = 'mysql -h ' + target_server + ' -P' + target_mysql_port + ' -u' + target_mysql_user + ' --password=' +  target_mysql_password + ' ' +dbname + ' < ' +  f 
    os.system(cmd)
    print "--> Uploaded"
  target_mysql_con.close()
  source_mysql_con.close()
  return 
  

def remove_old_tables(blog_id):
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  + '_commentmeta')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_comments')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_links')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_options')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_postmeta')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_posts')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_term_relationships')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_terms')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_term_taxonomy')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_tdomf_table_edits')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_tdomf_table_forms')
  exec_command_in_target_mysql('DROP TABLE IF EXISTS wp_'+ blog_id  +'_tdomf_table_widgets')


def exec_command_in_target_mysql(prm_cmd):
  debug_log(prm_cmd)
  cmd = 'mysql -h ' + target_server + ' -P' + target_mysql_port + ' -u' + target_mysql_user + ' --password=' +  target_mysql_password + '  ' + target_mysql_db + ' -e "' + prm_cmd  + '"'
  os.system(cmd)
  
  
def read_parameter_file(parameter_file_path):
  f = open(parameter_file_path,'rU')
  for line in f:
    if line.count('source_server') > 0:
      global source_server
      source_server = extract_parameter_value(line)
      
    elif  line.count('source_os_user') > 0:
      global source_os_user
      source_os_user = extract_parameter_value(line)
      
    elif line.count('source_os_password') > 0:
      global source_os_password
      source_os_password = extract_parameter_value(line)
      
    elif line.count('source_db_backup_root_dir') > 0:
      global source_db_backup_root_dir
      source_db_backup_root_dir = extract_parameter_value(line)
      global source_dbbackupdir
      source_dbbackupdir = source_db_backup_root_dir
      
    elif line.count('source_domain') > 0:
      global source_domain
      source_domain = extract_parameter_value(line)
    
    elif line.count('source_mysql_user') > 0:
      global source_mysql_user
      source_mysql_user = extract_parameter_value(line) 
    
    elif line.count('source_mysql_passwd') > 0:
      global source_mysql_passwd
      source_mysql_passwd = extract_parameter_value(line) 
    
    elif line.count('source_mysql_db') > 0:
      global source_mysql_db
      source_mysql_db = extract_parameter_value(line) 
    
    elif line.count('source_mysql_port') > 0:
      global source_mysql_port
      source_mysql_port = extract_parameter_value(line) 
    
    elif line.count('import_blog_dir') > 0:
      global import_blog_dir
      import_blog_dir = extract_parameter_value(line)
      
    elif line.count('target_server') > 0:
      global target_server
      target_server = extract_parameter_value(line)  
      
    elif line.count('target_mysql_user') > 0:
      global target_mysql_user
      target_mysql_user = extract_parameter_value(line)    
      
    elif line.count('target_mysql_password') > 0:
      global target_mysql_password
      target_mysql_password = extract_parameter_value(line)    
    
    
    elif line.count('target_mysql_port') > 0:
      global target_mysql_port
      target_mysql_port = extract_parameter_value(line)      
      
    elif line.count('hyperdb_factor') > 0:
      global hyperdb_factor
      hyperdb_factor = extract_parameter_value(line)        
      hyperdb_factor=int(hyperdb_factor)
        
def extract_parameter_value(prm_val):
   prm_val = prm_val[prm_val.index('=')+1:]
   prm_val = prm_val.replace("'","")
   prm_val = prm_val.strip()   
   return prm_val   
      
def display_conf_values():
  print "source_server=",source_server
  print "source_os_user=",source_os_user
  print "Source OS Password=",source_os_password
  print "source_db_backup_root_dir=",source_db_backup_root_dir
  print "source_dbbackupdir=",source_dbbackupdir
  print "source_domain=",source_domain
  print "source_mysql_user=",source_mysql_user
  print "source_mysql_passwd=",source_mysql_passwd
  print "source_mysql_port=",source_mysql_port
  print "source_mysql_db=",source_mysql_db
  
  print "import_blog_dir=",import_blog_dir
  print "target_server=",target_server
  print "target_mysql_user=",target_mysql_user
  print "target_mysql_password=",target_mysql_password
  print "target_mysql_port=",target_mysql_port
  print "hyperdb_factor=",hyperdb_factor


def create_sample_conf_file(filename):
   conf = 'source_server=0.0.0.0'
   conf = conf + "\nsource_os_user=root"
   conf = conf + "\nsource_os_password='xxxx'"
   conf = conf + "\nsource_db_backup_root_dir='/BACKUP'"
   conf = conf + "\nsource_domain='sports.com'"
   conf = conf + "\nsource_mysql_user='wpuser'"
   conf = conf + "\nsource_mysql_passwd='wpuser'"
   conf = conf + "\nsource_mysql_db='wordpress'"
   conf = conf + "\nsource_mysql_port='3306'"
   conf = conf + "\nimport_blog_dir='/tmp/import_blogs'"
   conf = conf + "\ntarget_server='10.0.0.1'"
   conf = conf + "\ntarget_mysql_user='wpuser'"
   conf = conf + "\ntarget_mysql_password='xxxx'"
   conf = conf + "\ntarget_mysql_port='3307'"
   conf = conf + "\nhyperdb_factor = 1000"
   f = open(filename,'w')
   f.write(conf)
   f.close()

def check_wp_blogs(blog_id, target_mysql_con, source_mysql_con):

  con = target_mysql_con
  cur = con.cursor()
  qry= "SELECT blog_id FROM wordpress.wp_blogs WHERE blog_id = " + str(blog_id)
  cur.execute(qry)
  numrows = int(cur.rowcount)
  row = import_wp_blogs_record(blog_id,source_mysql_con)
  if numrows > 0:
    cur.execute("DELETE FROM   wordpress.wp_blogs WHERE blog_id = " + str(blog_id))
  v_domain = row["domain"]
  v_domain = v_domain.replace(source_domain,target_domain)
  cur.execute( "INSERT INTO wordpress.wp_blogs(blog_id,site_id,domain,path,registered,last_updated,blog_name,type_of_entity,sort_order,display_short_name) VALUES (%s, %s, %s, %s, %s, %s , %s , %s , %s , %s)",(row["blog_id"], row["site_id"], v_domain, row["path"],row["registered"],  row["last_updated"], row["blog_name"], row["type_of_entity"], row["sort_order"], row["display_short_name"]  ) )       
  
  cur.close()

  return row     
   
    

def import_wp_blogs_record(blog_id,source_mysql_con):
  debug_log("In import_wp_blogs_record with blog_id = " + blog_id)
  con = source_mysql_con
  cur = con.cursor(mdb.cursors.DictCursor)
  qry= "SELECT blog_id,site_id,domain,path,registered,last_updated,blog_name,type_of_entity,sort_order,display_short_name FROM wordpress.wp_blogs WHERE blog_id = " + str(blog_id)
  debug_log(qry)
  cur.execute(qry)
  data = cur.fetchone()
  cur.close()
  debug_log("Return data: " + str(data))
  return data
  
def check_and_import_admin_school_league_region(blog_id, type_of_entity, target_mysql_con, source_mysql_con):
  
  debug_log("In check_and_import_admin_school_league_region with blog_id " + str(blog_id) + " Entity Type = " + str(type_of_entity))
  if type_of_entity == 3:
    v_table_name = 'admin_school'
    v_primary_col = 'school_id'
  elif type_of_entity == 9:
    v_table_name = 'admin_league'
    v_primary_col = 'league_id'
  elif type_of_entity == 10:
    v_table_name = 'admin_region'
    v_primary_col = 'sports_id'
    
    
  cur = source_mysql_con.cursor(mdb.cursors.DictCursor)
  qry= "SELECT blog_id, " + v_primary_col + " as primary_col FROM admin_data." + v_table_name + " WHERE blog_id = " + str(blog_id)
  cur.execute(qry)
  data = cur.fetchone()
  v_primary_id = data["primary_col"]
  debug_log(v_primary_col + " = " + str(v_primary_id))
  cur.close()
  
  
  cur = target_mysql_con.cursor()
  qry= "SELECT blog_id FROM admin_data."+ v_table_name + " WHERE "+ v_primary_col + " = " + str(v_primary_id)
  debug_log(qry)
  cur.execute(qry)
  numrows = int(cur.rowcount)
  
  if numrows > 0:
    qry = "DELETE FROM   admin_data." + v_table_name + " WHERE " + v_primary_col + " = " + str(v_primary_id)
    debug_log(qry)
    cur.execute(qry)
  
  cmd = 'mysqldump -h ' + source_server + ' -P' + source_mysql_port + ' -u' + source_mysql_user + ' --password=' +  source_mysql_passwd + ' admin_data  --tables ' + v_table_name + ' -w blog_id=' + blog_id + ' -t > /tmp/import_blog.sql '
  debug_log(cmd)  
  os.system(cmd)
  
  cmd = 'mysql -h ' + target_server + ' -P' + target_mysql_port + ' -u' + target_mysql_user + ' --password=' +  target_mysql_password + ' admin_data ' + ' < /tmp/import_blog.sql '
  debug_log(cmd)  
  os.system(cmd)  
    
  return  
    
def check_and_import_admin_team(blog_id, target_mysql_con, source_mysql_con):
  
  cur = source_mysql_con.cursor(mdb.cursors.DictCursor)
  qry= "SELECT blog_id, aggregated_team_id FROM admin_data.admin_team WHERE blog_id = " + str(blog_id)
  cur.execute(qry)
  data = cur.fetchone()
  v_aggregated_team_id = data["aggregated_team_id"]
  print v_aggregated_team_id
  cur.close()
  
  
  cur = target_mysql_con.cursor()
  qry= "SELECT blog_id FROM admin_data.admin_team WHERE aggregated_team_id = " + str(v_aggregated_team_id)
  print qry
  cur.execute(qry)
  numrows = int(cur.rowcount)
  
  if numrows > 0:
    qry = "DELETE FROM   admin_data.admin_team WHERE aggregated_team_id = " + str(v_aggregated_team_id) 
    print qry
    cur.execute(qry)
    cur.execute("DELETE FROM   admin_data.admin_team_details WHERE aggregated_team_id = " + str(v_aggregated_team_id) )
    
  cur.close()    
  
    
  cmd = 'mysqldump -h ' + source_server + ' -P' + source_mysql_port + ' -u' + source_mysql_user + ' --password=' +  source_mysql_passwd + ' admin_data  --tables admin_team -w blog_id=' + blog_id + ' -t > /tmp/import_blog.sql '
  debug_log(cmd)
  os.system(cmd)
  
  
  cmd = 'mysql -h ' + target_server + ' -P' + target_mysql_port + ' -u' + target_mysql_user + ' --password=' +  target_mysql_password + ' admin_data ' + ' < /tmp/import_blog.sql '
  debug_log(cmd)
  os.system(cmd)
  
  os.system('rm -f /tmp/import_blog.sql')
  
  cmd = 'mysqldump -h ' + source_server + ' -P' + source_mysql_port + ' -u' + source_mysql_user + ' --password=' +  source_mysql_passwd + ' admin_data  --tables admin_team_details -w "aggregated_team_id = ' + str(v_aggregated_team_id) +  '" -t --skip-opt > /tmp/import_blog.sql '
  debug_log(cmd)
  os.system(cmd)
  
  
  cmd = 'mysql -h ' + target_server + ' -P' + target_mysql_port + ' -u' + target_mysql_user + ' --password=' +  target_mysql_password + ' admin_data   < /tmp/import_blog.sql '
  debug_log(cmd)
  os.system(cmd)
  
  return  

def find_dependent_teams(blog_id,source_mysql_con,blog_id_list):
  
  cur = source_mysql_con.cursor(mdb.cursors.DictCursor)
  qry = "SELECT t.blog_id FROM admin_data.admin_team as t Inner Join  admin_data.admin_school as s ON t.school_id = s.school_id WHERE t.blog_id is not null and s.blog_id = " + blog_id + " limit " + str(number_of_deps)  + ")"
  cur.execute(qry)  
  numrows = int(cur.rowcount)
  for i in range(numrows):
    row = cur.fetchone()
    v_blog_id = str(row["blog_id"])
    blog_id_list.append(v_blog_id)
  
  cur.close()
  return blog_id_list



def find_dependent_blogs(blog_id_list):
  
  source_mysql_con = mdb.connect(host=source_server, user=source_mysql_user,passwd=source_mysql_passwd,db=source_mysql_db,port=int(source_mysql_port))
  print blog_id_list
  cur = source_mysql_con.cursor(mdb.cursors.DictCursor)
  for blog_id in blog_id_list:
    
    qry= "SELECT blog_id,type_of_entity FROM wordpress.wp_blogs WHERE blog_id = " + str(blog_id)
    cur.execute(qry)
    data = cur.fetchone()
    v_type_of_entity = data["type_of_entity"]
    print v_type_of_entity
    
    
    if v_type_of_entity == 3: #schools
      #blog_id_list = find_dependent_teams(blog_id,source_mysql_con, blog_id_list)
      qry = "SELECT t.blog_id FROM admin_data.admin_team as t Inner Join  admin_data.admin_school as s ON t.school_id = s.school_id WHERE t.blog_id is not null and s.blog_id = " + blog_id + " limit " + str(number_of_deps)  + ")"
    elif v_type_of_entity == 9: #leagues
      qry = ("(SELECT t.blog_id " 
            " FROM admin_data.admin_team as t " 
            " Inner Join  admin_data.admin_school as s ON t.school_id = s.school_id " 
            " Inner Join admin_data.admin_league as l on s.league_id = l.league_id " 
            " WHERE t.blog_id is not null and l.blog_id = " + str(blog_id) + " limit " + str(number_of_deps)  + ")"
            " union all " 
            " (SELECT s.blog_id " 
            " FROM admin_data.admin_school as s " 
            " Inner Join admin_data.admin_league as l on s.league_id = l.league_id " 
            " WHERE s.blog_id is not null and l.blog_id = " + str(blog_id) + " limit " + str(number_of_deps)  + ")" 
            )
      
    else:
      qry = ( "(SELECT t.blog_id "
            " FROM admin_data.admin_team as t "
            " Inner Join  admin_data.admin_school as s ON t.school_id = s.school_id "
            " Inner Join admin_data.admin_league as l on s.league_id = l.league_id "
            " Inner Join admin_data.admin_region as r on l.region_sports_id = r.sports_id "
            " WHERE t.blog_id is not null and r.blog_id = " + str(blog_id) + " limit " + str(number_of_deps)  + ")" 
            " union all "
            " (SELECT s.blog_id "
            " FROM admin_data.admin_school as s "
            " Inner Join admin_data.admin_league as l on s.league_id = l.league_id "
            " Inner Join admin_data.admin_region as r on l.region_sports_id = r.sports_id "
            " WHERE s.blog_id is not null and r.blog_id = " + str(blog_id) + " limit " + str(number_of_deps)  + ")" 
            " union all "
            " (SELECT l.blog_id "
            " FROM  admin_data.admin_league as l "
            " Inner Join admin_data.admin_region as r on l.region_sports_id = r.sports_id "
            " WHERE l.blog_id is not null and r.blog_id = " + str(blog_id) + " limit " + str(number_of_deps)  + ")" 
            )

            
    debug_log(qry)
    cur.execute(qry)  
    rows = cur.fetchall()
      
  for r in rows:
    blog_id_list.append(str(r["blog_id"]))
           
  cur.close()
      
    
  print blog_id_list
  
  source_mysql_con.close()
  
def main():
  
  parser = OptionParser(usage="usage: %prog [options] blog_id_1, blog_id_2, ... blog_id_N")
  parser.add_option("-p",action="store", dest="parameter_file_path", help="Read configuration parameter from file")
  parser.add_option("-c", "--create-conf-file",action="store", dest="create_conf_file", help="Create sample configuration parameter file")
  parser.add_option("-d", "--debug",action="store_true", dest="debug_mode", help="Run in debug mode")
  parser.add_option("--with-deps",action="store", dest="number_of_deps", help="Number of dependent league/school/team to be imported")
  
  (options, args) = parser.parse_args()
  
  if options.number_of_deps:
    global number_of_deps
    number_of_deps = options.number_of_deps
  
  global debug_mode
  debug_mode = options.debug_mode
  
  #CREATE CONF FILE
  global create_conf_file
  create_conf_file = options.create_conf_file
  if create_conf_file and len(create_conf_file) > 0:
    create_sample_conf_file(create_conf_file)
    print "Configuration file created"
    exit()
  
  #USE parameter_file_path
  global parameter_file_path
  parameter_file_path = options.parameter_file_path
  #print parameter_file_path
  
  if not parameter_file_path:
    print "!!!Please provide configuration parameter file path!!!"
    parser.print_help()
    return
  read_parameter_file(parameter_file_path)
  if debug_mode:
    display_conf_values()
  
  blog_id_list = args
  
  find_dependent_blogs(blog_id_list)
  
  
  print ""
  print "Opening connection to source server ..."
  ssh = open_ssh_connection()
  print "Done"
  print ""
  
  print "Searching for latest backup directory in source server ..."
  source_dbbackupdir = find_backup_dir(ssh)
  print "Lastest backup directory =", source_dbbackupdir
  print "Done"
  print ""
  
  print "Search for backup files of the blogs..."
  blog_backup_path_dict = find_backup_file_for_blogs(blog_id_list,ssh,source_dbbackupdir,hyperdb_factor)
  print "--- Blogs to import ---"
  blog_backup_path_dict = remove_wrong_blog_id_from_list(blog_backup_path_dict)
  display_dict(blog_backup_path_dict)
  print "Done"
  print ""

  if len(blog_backup_path_dict) == 0:
    exit()
    
  print "Make a tar file of the backup files of blogs..."
  blogs_gz = make_tar_file_from_blog_list(ssh,blog_backup_path_dict)
  print "Done"
  print ""
  
  print "Download the tar file from the source server to local machine..."
  local_blog_gz = copy_file_from_remote_machine(ssh,blogs_gz)
  print "Done"
  print ""
  
  print "Closing connection with the source server..."
  close_ssh_connection(ssh)
  print "Done"
  print ""
  
  print "Extract the tar file..."
  import_blog_dir = extract_import_blogs_gz(local_blog_gz)
  print "Done"
  print ""
  
  print "Find targeted server domain..."
  global target_domain
  target_domain = find_target_domain()
  print "-> Targeted server domain =",target_domain
  print "Done"
  print ""
  
  print "Make the downloaded dump file ready for target server..."
  import_blog_dir = make_dumpfiles_for_target_domain(import_blog_dir,target_domain,source_domain)
  print "Done"
  print ""
  
  print "Upload the backup dump files to target mysql instance..."
  upload_to_mysqldb(import_blog_dir,hyperdb_factor)
  print "Done"
  print ""
  
  return
  
  
if __name__ == '__main__':
 main()
