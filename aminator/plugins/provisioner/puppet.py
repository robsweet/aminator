# -*- coding: utf-8 -*-

#
#
#  Copyright 2013 Netflix
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#
#

"""
aminator.plugins.provisioner.puppet
================================
"""
import os
import shutil
import time
import socket
import logging
from collections import namedtuple
import json

from aminator.plugins.provisioner.base import BaseProvisionerPlugin
from aminator.plugins.provisioner.apt import AptProvisionerPlugin, dpkg_install, apt_get_update, apt_get_install
from aminator.plugins.provisioner.yum import YumProvisionerPlugin, yum_localinstall, yum_install, yum_clean_metadata
from aminator.util import download_file
from aminator.util.linux import command, mkdirs
from aminator.util.linux import Chroot
from aminator.config import conf_action

__all__ = ('PuppetProvisionerPlugin',)
log = logging.getLogger(__name__)

CommandResult = namedtuple('CommandResult', 'success result')
CommandOutput = namedtuple('CommandOutput', 'std_out std_err')


class PuppetProvisionerPlugin(BaseProvisionerPlugin):
    """
    PuppetProvisionerPlugin takes the majority of its behavior from BaseProvisionerPlugin
    See BaseProvisionerPlugin for details
    """
    _name = 'puppet'

    def add_plugin_args(self):
        context = self._config.context
        puppet_config = self._parser.add_argument_group(title='Puppet Options',
                                                      description='Options for the puppet provisioner')

        puppet_config.add_argument('-P', '--puppet-master-hostname', dest='puppet_master_hostname',
                                    action=conf_action(config=context.puppet),
                                    help='The puppet master hostname')


        puppet_config.add_argument('-A', '--puppet_apply_args', dest='puppet_apply_args',
                                    action=conf_action(config=context.puppet),
                                    help='Extra arguments for Puppet Apply.  Can be used to include a Puppet class with -e.')

        puppet_config.add_argument('-M', '--puppet_manifests', dest='puppet_manifests',
                                    action=conf_action(config=context.puppet),
                                    help='Puppet manifests to apply.  This can be a tarball or a single pp file.')

    def _store_package_metadata(self):
        ""
        
    def _provision_package(self):
        ""
        
    def _pre_chroot_block(self):
        log.debug('Starting _pre_chroot_block')
        context = self._config.context
        config = self._config
        
        log.debug("Setting metadata release to {0}".format(time.strftime("%Y%m%d%H%M")))
        context.package.attributes = {'name': '', 'version': 'puppet', 'release': time.strftime("%Y%m%d%H%M") }

        if self._puppet_run_mode is 'master':
            generate_certificate(context.package.arg)
            self.make_puppet_certs_dir()
            self.copy_puppet_certs(context.package.arg)
        elif self._puppet_run_mode is 'apply':
            self.copy_puppet_manifests(context.puppet.get('puppet_manifests'))
                                                                            
    def _makedirs(self, dirs):
        log.debug('creating directory {0} if it does not exist'.format(dirs))
        if not os.path.exists(dirs):
            os.makedirs(dirs)


    def make_puppet_certs_dir(self, certs_dir = '/var/lib/puppet/ssl/certs', private_keys_dir = '/var/lib/puppet/ssl/private_keys'):
        self._makedirs(self._distro._mountpoint + certs_dir)
        self._makedirs(self._distro._mountpoint + private_keys_dir)

    def copy_puppet_certs(self, pem_file_name, certs_dir = '/var/lib/puppet/ssl/certs', private_keys_dir = '/var/lib/puppet/ssl/private_keys'):
        # TODO make this configurable
        log.debug('Placing certs for {0} into mountpoint {1}'.format(pem_file_name, self._distro._mountpoint))
        shutil.copy(certs_dir        + '/ca.pem',           self._distro._mountpoint + certs_dir)
        shutil.copy(certs_dir        + '/' + pem_file_name + '.pem', self._distro._mountpoint + certs_dir)
        shutil.copy(private_keys_dir + '/' + pem_file_name + '.pem', self._distro._mountpoint + private_keys_dir)

    def rm_puppet_certs_dirs(self, certs_dir = '/var/lib/puppet/ssl'):
        shutil.rmtree(certs_dir)

    def copy_puppet_manifests(self, manifests):
        import tarfile
        import shutil

        if tarfile.is_tarfile(manifests):
            self._puppet_apply_file = ''
            tar = tarfile.open(manifests)

            dest_dir = os.path.join(self._distro._mountpoint,'etc','puppet') if 'modules' in tar.getnames() else os.path.join(self._distro._mountpoint,'etc','puppet','modules')

            self._makedirs(dest_dir)
            log.debug('Untarring to {0}'.format(dest_dir))
            os.chdir(dest_dir)
            tar.extractall
            tar.close
        else:
            self._puppet_apply_file = os.path.join('etc','puppet','modules', os.path.basename(manifests))
            dest_file = os.path.join(self._distro._mountpoint,'etc','puppet','modules', os.path.basename(manifests))
            self._makedirs(os.path.join(self._distro._mountpoint,'etc','puppet','modules'))
            log.debug('Trying to copy \'{0}\' to \'{1}\''.format(manifests, dest_file))
            shutil.copy2(manifests, dest_file)

    def provision(self):
        log.debug('Starting provision')
        if self._config.context.puppet.get('puppet_master_hostname') is not None:
           self._puppet_run_mode = 'master'
        elif self._config.context.puppet.get('puppet_manifests') is not None:
           self._puppet_run_mode = 'apply'
        else:
           log.exception('Must pass either puppet_master_hostname or puppet_manifests')
           return False
        
        log.debug('Puppet run mode = {0}'.format(self._puppet_run_mode))

        """
        overrides the base provision
      * generate certificates
      * install the certificates on the target volume
          * install puppet on the target volume
      * run the puppet agent in the target chroot environment
        """

        context = self._config.context
        config = self._config

        self._pre_chroot_block()
        
        log.debug('Entering chroot at {0}'.format(self._distro._mountpoint))

        with Chroot(self._distro._mountpoint):
            log.debug('Inside Puppet chroot')


            if self._distro._name is 'redhat':
                log.info('Installing Puppet with yum.')
                yum_clean_metadata
                yum_install('puppet')
            else:
                log.info('Installing Puppet with apt.')
                apt_get_update
                apt_get_install('puppet')

            if self._puppet_run_mode is 'master':
                log.info('Running puppet agent')
                result = puppet_agent(context.package.arg, context.puppet.get('puppet_master_hostname', socket.gethostname()))
                self.rm_puppet_certs_dirs()
            elif self._puppet_run_mode is 'apply':
                if self._puppet_apply_file is '':
                    log.info('Running puppet apply')
                else:
                    log.info('Running puppet apply for {0}'.format(self._puppet_apply_file))
                result = puppet_apply( context.puppet.get('puppet_apply_args', '--debug'), self._puppet_apply_file )

            # * --detailed-exitcodes:
            #   Provide transaction information via exit codes. If this is enabled, an exit
            #   code of '2' means there were changes, an exit code of '4' means there were
            #   failures during the transaction, and an exit code of '6' means there were both
            #   changes and failures.
            log.info('Puppet status code {0} with result {1}'.format(result.result.status_code, result.result))
            if not (result.result.status_code in [0,2]):
                log.critical('Puppet run failed: {0.std_err}'.format(result.result))
                return False

        log.debug('Exited chroot')

        log.info('Provisioning succeeded!')

        return True


@command()
def puppet_agent(certname, puppet_master_hostname):
    return 'puppet agent --detailed-exitcodes --no-daemonize --logdest console --onetime --certname {0} --server {1}'.format(certname, puppet_master_hostname)

@command()
def puppet_apply( puppet_apply_args, puppet_apply_file ):
    return 'puppet apply --detailed-exitcodes --logdest console --verbose {0} {1}'.format(puppet_apply_args, puppet_apply_file)

@command()
def generate_certificate(certname):
    log.debug('Generating certificate for {0}'.format(certname))
    return 'puppetca generate {0}'.format(certname)




