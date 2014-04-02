# Copyright 2009-2013 Justin Riley
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

import posixpath

from starcluster import threadpool
from starcluster import clustersetup
from starcluster.logger import log

core_site_templ = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<!-- In: conf/core-site.xml -->
<property>
  <name>hadoop.tmp.dir</name>
  <value>%(hadoop_tmpdir)s</value>
  <description>A base for other temporary directories.</description>
</property>

<property>
  <name>fs.default.name</name>
  <value>hdfs://%(master)s:8020/</value>
  <description>The name of the default file system.  A URI whose
  scheme and authority determine the FileSystem implementation.  The
  uri's scheme determines the config property (fs.SCHEME.impl) naming
  the FileSystem implementation class.  The uri's authority is used to
  determine the host, port, etc. for a filesystem.</description>
</property>
<property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
</property>

</configuration>
"""

hdfs_site_templ = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<!-- In: conf/hdfs-site.xml -->
<property>
        <name>dfs.permissions</name>
        <value>false</value>
</property>
<!-- <property>
  <name>dfs.permissions.superusergroup</name>
  <value>hadoop</value>
</property> -->
<property>
  <name>dfs.name.dir</name>
  <value>%(hadoop_tmpdir)s/name</value>
</property>
<property>
  <name>dfs.data.dir</name>
  <value>%(hadoop_tmpdir)s/data</value>
</property>
<property>
  <name>dfs.replication</name>
  <value>%(replication)d</value>
  <description>Default block replication.
  The actual number of replications can be specified when the file is created.
  The default is used if replication is not specified in create time.
  </description>
</property>
<property>
  <name>dfs.namenode.http-address</name>
  <value>%(master)s:50070</value>
  <description>
    The address and the base port on which the dfs NameNode Web UI will listen.
  </description>
</property>
<property>
        <name>dfs.blocksize</name>
        <value>268435456</value>
</property>
<property>
        <name>dfs.namenode.handler.count</name>
        <value>100</value>
</property>

</configuration>
"""

mapred_site_templ = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<!-- In: conf/mapred-site.xml -->
<property>
  <name>mapreduce.framework.name</name>
  <value>yarn</value>
</property>
<property>
  <name>mapreduce.tasktracker.map.tasks.maximum</name>
  <value>%(map_tasks_max)d</value>
</property>
<property>
  <name>mapreduce.tasktracker.reduce.tasks.maximum</name>
  <value>%(reduce_tasks_max)d</value>
</property>
<property>
        <name>mapred.child.java.opts</name>
        <value>-Xmx1024m</value>
</property>
<property>
        <name>mapreduce.task.io.sort.factor</name>
        <value>50</value>
</property>
<property>
        <name>mapreduce.task.io.sort.mb</name>
        <value>500</value>
</property>
<property>
        <name>mapreduce.reduce.shuffle.parallelcopies</name>
        <value>15</value>
</property>
<property>
        <name>mapreduce.jobhistory.address</name>
        <value>%(master)s:10020</value>
</property>
<property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>%(master)s:19888</value>
</property>

</configuration>
"""
yarn_site_templ = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<!-- In: conf/yarn-site.xml -->
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce.shuffle</value>
  </property>

  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>

  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>%(master)s</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>%(master)s:8032</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>%(master)s:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>%(master)s:8031</value>
  </property>
  <property>
    <name>yarn.resourcemanager.admin.address</name>
    <value>%(master)s:8033</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>%(master)s:8088</value>
  </property>
  <property>
    <description>List of directories to store localized files in.</description>
    <name>yarn.nodemanager.local-dirs</name>
    <value>%(hadoop_tmpdir)s/hadoop-yarn/cache/${user.name}/nm-local-dir</value>
  </property>

  <property>
    <description>Where to store container logs.</description>
    <name>yarn.nodemanager.log-dirs</name>
    <value>%(hadoop_tmpdir)s/hadoop-yarn/log/containers</value>
  </property>

  <property>
    <description>Where to aggregate logs to.</description>
    <name>yarn.nodemanager.remote-app-log-dir</name>
    <value>%(hadoop_tmpdir)s/hadoop-yarn/log/apps</value>
  </property>

  <property>
    <description>Classpath for typical applications.</description>
     <name>yarn.application.classpath</name>
     <value>
        $HADOOP_CONF_DIR,
        $HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,
        $HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,
        /usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*,
        $YARN_HOME/*,$YARN_HOME/lib/*
     </value>
  </property>


</configuration>
"""


class Hadoop2(clustersetup.ClusterSetup):
    """
    Configures Hadoop using Cloudera packages on StarCluster
    """

    def __init__(self, hadoop_tmpdir='/mnt/hadoop', map_to_proc_ratio='1.0',
                 reduce_to_proc_ratio='0.3'):
        
        self.hadoop_tmpdir = hadoop_tmpdir
        self.hadoop_home = '/usr/lib/hadoop'
        self.hadoop_conf = '/etc/hadoop/conf.starcluster'
        self.hdfs_historydir = '/user/history'
        self.hdfs_logdir='/var/log/hadoop-yarn'
        self.hdfs_tmpdir = '/tmp'
        self.empty_conf = '/etc/hadoop/conf.empty'
        
        self.centos_java_home = '/usr/lib/jvm/java'
        self.centos_alt_cmd = 'alternatives'
        self.ubuntu_javas = ['/usr/lib/jvm/java-6-sun/jre',
                             '/usr/lib/jvm/java-6-openjdk/jre',
                             '/usr/lib/jvm/java-7-openjdk/jre',
                             '/usr/lib/jvm/java-7-openjdk-amd64/jre',
                             '/usr/lib/jvm/default-java/jre']
        self.ubuntu_alt_cmd = 'update-alternatives'
        self.map_to_proc_ratio = float(map_to_proc_ratio)
        self.reduce_to_proc_ratio = float(reduce_to_proc_ratio)
        self.reduce_to_mem_ratio = 0.25
        self.map_to_mem_ratio = 1.0
        self._pool = None

    @property
    def pool(self):
        if self._pool is None:
            self._pool = threadpool.get_thread_pool(20, disable_threads=False)
        return self._pool

    def _get_java_home(self, node):
        # check for CentOS, otherwise default to Ubuntu 10.04's JAVA_HOME
        if node.ssh.isfile('/etc/redhat-release'):
            return self.centos_java_home
        for java in self.ubuntu_javas:
            if node.ssh.isdir(java):
                return java
        raise Exception("Cant find JAVA jre")

    def _get_alternatives_cmd(self, node):
        # check for CentOS, otherwise default to Ubuntu 10.04
        if node.ssh.isfile('/etc/redhat-release'):
            return self.centos_alt_cmd
        return self.ubuntu_alt_cmd

    def _setup_hadoop_user(self, node, user):
        node.ssh.execute('gpasswd -a %s hadoop' % user)
    
    def _setup_sysctl(self,node):
        node.ssh.execute('sysctl -w net.ipv6.conf.all.disable_ipv6=1')
        node.ssh.execute('sysctl -w net.ipv6.conf.default.disable_ipv6=1')
        node.ssh.execute('sysctl -w net.ipv6.conf.lo.disable_ipv6=1')
        node.ssh.execute('sysctl -w net.core.somaxconn=512')

    def _install_empty_conf(self, node):
        node.ssh.execute('cp -r %s %s' % (self.empty_conf, self.hadoop_conf))
        alternatives_cmd = self._get_alternatives_cmd(node)
        cmd = '%s --install /etc/hadoop/conf ' % alternatives_cmd
        cmd += 'hadoop-conf %s 50' % self.hadoop_conf
        node.ssh.execute(cmd)

    def _configure_env(self, node):
        env_file_sh = posixpath.join(self.hadoop_conf, 'yarn-env.sh')
        node.ssh.remove_lines_from_file(env_file_sh, 'JAVA_HOME')
        env_file = node.ssh.remote_file(env_file_sh, 'a')
        env_file.write('export JAVA_HOME=%s\n' % self._get_java_home(node))
        env_file.close()    

    def _configure_mapreduce_site(self, node, cfg):
        mapred_site_xml = posixpath.join(self.hadoop_conf, 'mapred-site.xml')
        mapred_site = node.ssh.remote_file(mapred_site_xml)
        # Hadoop default: 2 maps, 1 reduce
        # AWS EMR uses approx 1 map per proc and .3 reduce per proc
        map_tasks_max = max(
            2,
            int(self.map_to_mem_ratio * node.memory/1024))
        reduce_tasks_max = max(
            1,
            int(self.reduce_to_mem_ratio * node.memory/1024))
        cfg.update({
            'map_tasks_max': map_tasks_max,
            'reduce_tasks_max': reduce_tasks_max})
        mapred_site.write(mapred_site_templ % cfg)
        mapred_site.close()

    def _configure_core(self, node, cfg):
        core_site_xml = posixpath.join(self.hadoop_conf, 'core-site.xml')
        core_site = node.ssh.remote_file(core_site_xml)
        core_site.write(core_site_templ % cfg)
        core_site.close()

    def _configure_hdfs_site(self, node, cfg):
        hdfs_site_xml = posixpath.join(self.hadoop_conf, 'hdfs-site.xml')
        hdfs_site = node.ssh.remote_file(hdfs_site_xml)
        hdfs_site.write(hdfs_site_templ % cfg)
        hdfs_site.close()
    
    def _configure_yarn_site(self, node, cfg):
        yarn_site_xml = posixpath.join(self.hadoop_conf, 'yarn-site.xml')
        yarn_site = node.ssh.remote_file(yarn_site_xml)
        yarn_site.write(yarn_site_templ % cfg)
        yarn_site.close()

    def _configure_masters(self, node, master):
        masters_file = posixpath.join(self.hadoop_conf, 'masters')
        masters_file = node.ssh.remote_file(masters_file)
        masters_file.write(master.alias)
        masters_file.write('\n')
        masters_file.close()

    def _configure_slaves(self, node, master, node_aliases):
        slaves_file = posixpath.join(self.hadoop_conf, 'slaves')
        slaves_file = node.ssh.remote_file(slaves_file)
        if node != master:
            slaves_file.write('\n'.join(node_aliases))
        slaves_file.write('\n')
        slaves_file.close()

    def _setup_hdfs(self, node, user):
        
        node.ssh.execute("umount /mnt")
        node.ssh.execute("ls /dev/xvda[a-z] | xargs pvcreate")
        node.ssh.execute("pvscan | awk '{if(NF==5){print $2}}' | xargs vgcreate vg")
        node.ssh.execute("lvcreate -i $(pvscan | awk '/Total/{print $2}') -n lv -l100%VG vg")
        node.ssh.execute("mkfs.ext4 /dev/vg/lv")
        node.ssh.execute("mount -t ext4 -o noatime /dev/vg/lv /mnt")
        
        self._setup_hadoop_dir(node, self.hadoop_tmpdir, 'hdfs', 'hadoop')
        mapred_dir = posixpath.join(self.hadoop_tmpdir, 'hadoop-mapred')
        self._setup_hadoop_dir(node, mapred_dir, 'mapred', 'hadoop')
        userdir = posixpath.join(self.hadoop_tmpdir, 'hadoop-%s' % user)
        self._setup_hadoop_dir(node, userdir, user, 'hadoop')
        hdfsdir = posixpath.join(self.hadoop_tmpdir, 'hadoop-hdfs')
        self._setup_hadoop_dir(node, userdir, user, 'hadoop')
        hdfsdir = posixpath.join(self.hadoop_tmpdir, 'hadoop-yarn')
        if not node.ssh.isdir(hdfsdir):
            node.ssh.execute("su hdfs -c 'hadoop namenode -format'")
        self._setup_hadoop_dir(node, hdfsdir, 'hdfs', 'hadoop')      
        
        
    def _setup_dumbo(self, node):
        if not node.ssh.isfile('/etc/dumbo.conf'):
            f = node.ssh.remote_file('/etc/dumbo.conf')
            f.write('[hadoops]\nstarcluster: %s\n' % self.hadoop_home)
            f.close()

    def _configure_hadoop(self, master, nodes, user):
        log.info("Configuring Hadoop...")
        log.info("Setting up kernel parameters...")
        for node in nodes:
            self.pool.simple_job(self._setup_sysctl, node, jobid=node.alias)
        log.info("Adding user %s to hadoop group" % user)
        for node in nodes:
            self.pool.simple_job(self._setup_hadoop_user, (node, user),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        node_aliases = map(lambda n: n.alias, nodes)
        replication_factor=min(3,max(1,len(nodes)/3))
        cfg = {'master': master.alias, 'replication': replication_factor,
               'hadoop_tmpdir': posixpath.join(self.hadoop_tmpdir,
                                               'hadoop-${user.name}')}
        log.info("Installing configuration templates...")
        for node in nodes:
            self.pool.simple_job(self._install_empty_conf, (node,),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        #log.info("Configuring environment...")
#        for node in nodes:
#self.pool.simple_job(self._configure_env, (node,),
                                 #jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring YARN Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_yarn_site, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring MapReduce Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_mapreduce_site, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring Core Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_core, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring HDFS Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_hdfs_site, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring masters file...")
        for node in nodes:
            self.pool.simple_job(self._configure_masters, (node, master),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring slaves file...")
        for node in nodes:
            self.pool.simple_job(self._configure_slaves, (node, master, node_aliases),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring HDFS...")
        for node in nodes:
            self.pool.simple_job(self._setup_hdfs, (node, user),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring dumbo...")
        for node in nodes:
            self.pool.simple_job(self._setup_dumbo, (node,), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

    def _create_hdfs_directories(self, master):
        self._setup_hdfs_dir(master, self.hdfs_historydir, 'yarn', 'supergroup', permission="1777")
        self._setup_hdfs_dir(master, self.hdfs_logdir, 'yarn', 'mapred')
        

    def _setup_hadoop_dir(self, node, path, user, group, permission="775"):
        
        if not node.ssh.isdir(path):
            node.ssh.mkdir(path)
            
        node.ssh.execute("chown -R %s:hadoop %s" % (user, path))
        node.ssh.execute("chmod -R %s %s" % (permission, path))

    def _setup_hdfs_dir(self, node, path, user, group, permission="1775"):
        node.ssh.execute("su hdfs -c 'hadoop fs -mkdir %s'" % (path))
        node.ssh.execute("su hdfs -c 'hadoop fs -chown %s:%s %s'" % (user, group, path))
        node.ssh.execute("su hdfs -c 'hadoop fs -chmod -R %s %s'" % (permission, path))
        
    def _start_datanode(self, node):
        node.ssh.execute('sudo service hadoop-hdfs-datanode restart')

    def _start_nodemanager(self, node):
        node.ssh.execute('sudo service hadoop-yarn-nodemanager restart')

    def _start_hadoop(self, master, nodes):
        log.info("Starting namdnode in master...")
        master.ssh.execute('sudo service hadoop-hdfs-namenode restart')
        log.info("Starting secondary namenode in master...")
        master.ssh.execute('sudo service hadoop-hdfs-secondarynamenode restart')
        log.info("Starting datanode on all, but master, nodes...")
        for node in nodes:
            if node != master:
                self.pool.simple_job(self._start_datanode, (node,),
                                 jobid=node.alias)
        self.pool.wait()
 #       self._create_hdfs_directories(master)
        log.info("Starting resource manager in master...")
        
        master.ssh.execute('sudo service hadoop-yarn-resourcemanager restart')
        log.info("Starting node manager on all, but master, nodes...")
        for node in nodes:
            if node != master:
                self.pool.simple_job(self._start_nodemanager, (node,),
                                 jobid=node.alias)
        self.pool.wait()
        log.info("Starting history server in master...")
        master.ssh.execute('sudo service hadoop-mapreduce-historyserver restart')
    def _open_ports(self, master):
        ports = [50070, 50030]
        ec2 = master.ec2
        for group in master.cluster_groups:
            for port in ports:
                has_perm = ec2.has_permission(group, 'tcp', port, port,
                                              '0.0.0.0/0')
                if not has_perm:
                    ec2.conn.authorize_security_group(group_id=group.id,
                                                      ip_protocol='tcp',
                                                      from_port=port,
                                                      to_port=port,
                                                      cidr_ip='0.0.0.0/0')

    def run(self, nodes, master, user, user_shell, volumes):
        self._configure_hadoop(master, nodes, user)
        self._start_hadoop(master, nodes)
        self._open_ports(master)
        log.info("Job tracker status: http://%s:50030" % master.dns_name)
        log.info("Namenode status: http://%s:50070" % master.dns_name)
