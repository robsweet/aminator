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
aminator.plugins.provisioner.apt_puppet
================================
"""
import os
import shutil
import time
import socket
import logging
from collections import namedtuple
import json

from aminator.plugins.provisioner.apt import AptProvisionerPlugin, dpkg_install
from aminator.util import download_file
from aminator.util.linux import command, mkdirs, apt_get_update, apt_get_install
from aminator.util.linux import Chroot
from aminator.config import conf_action

__all__ = ('AptPuppetProvisionerPlugin',)
log = logging.getLogger(__name__)

CommandResult = namedtuple('CommandResult', 'success result')
CommandOutput = namedtuple('CommandOutput', 'std_out std_err')


class AptPuppetProvisionerPlugin(AptProvisionerPlugin):
    """
    AptPuppetProvisionerPlugin takes the majority of its behavior from AptProvisionerPlugin
    See AptProvisionerPlugin for details
    """
    _name = 'apt_puppet'

    def add_plugin_args(self):
        context = self._config.context
        puppet_config = self._parser.add_argument_group(title='Puppet Options',
                                                      description='Options for the puppet provisioner')

        puppet_config.add_argument('-P', '--puppet-master-hostname', dest='puppet_master_hostname',
                                    action=conf_action(config=context.puppet),
                                    help='The puppet master hostname')


    def _store_package_metadata(self):
        """
        save info for the AMI we're building in the context so that it can be incorporated into tags and the description
        during finalization. these values come from the chef JSON node file since we can't query the package for these
        attributes
        """
        context = self._config.context

	context.package.attributes = {'name': context.package.arg, 'version': 'puppet', 'release': time.strftime("%Y%m%d%H%M") }

    def _makedirs(self, dirs):
	log.debug('creating directory {0} if it does not exist'.format(dirs))    
        if not os.path.exists(dirs):
            os.makedirs(dirs)


    def make_puppet_certs_dir(self, certs_dir = '/var/lib/puppet/ssl/certs', private_keys_dir = '/var/lib/puppet/ssl/private_keys'):
        self._makedirs(self._mountpoint + certs_dir)
        self._makedirs(self._mountpoint + private_keys_dir)

    def copy_puppet_certs(self, pem_file_name, certs_dir = '/var/lib/puppet/ssl/certs', private_keys_dir = '/var/lib/puppet/ssl/private_keys'):
	# TODO make this configurable     
	log.debug('Placing certs for {0} into mountpoint {1}'.format(pem_file_name, self._mountpoint))
	shutil.copy(certs_dir        + '/ca.pem',           self._mountpoint + certs_dir)
	shutil.copy(certs_dir        + '/' + pem_file_name + '.pem', self._mountpoint + certs_dir)
	shutil.copy(private_keys_dir + '/' + pem_file_name + '.pem', self._mountpoint + private_keys_dir)

    def rm_puppet_certs_dirs(self, certs_dir = '/var/lib/puppet/ssl'):
        shutil.rmtree(certs_dir)

    def provision(self):
        """
        overrides the base provision
	  * generate certificates
	  * install the certificates on the target volume
          * install puppet on the target volume
	  * run the puppet agent in the target chroot environment
        """

        log.debug('Entering chroot at {0}'.format(self._mountpoint))

        context = self._config.context
        config = self._config

        # TODO
	# generate the certificate or check that the specified file exists
	generate_certificate(context.package.arg)

        self.make_puppet_certs_dir()
	self.copy_puppet_certs(context.package.arg)

        with Chroot(self._mountpoint):
            log.debug('Inside chroot')

            apt_get_update
            log.info('Installing puppet agent')
            apt_get_install("puppet")
            
            log.info('Running puppet agent')
            result = puppet(context.package.arg, context.puppet.get('puppet_master_hostname', socket.gethostname()))
            self.rm_puppet_certs_dirs()

            # * --detailed-exitcodes:
            #   Provide transaction information via exit codes. If this is enabled, an exit
            #   code of '2' means there were changes, an exit code of '4' means there were
            #   failures during the transaction, and an exit code of '6' means there were both
            #   changes and failures.
            log.info('puppet status code {0} with result {1}'.format(result.result.status_code, result.result))
            if not (result.result.status_code in [0,2]):
                log.critical('puppet agent run failed: {0.std_err}'.format(result.result))
                return False

            
            self._store_package_metadata()

        self.make_puppet_certs_dir()
        log.debug('Exited chroot')

        log.info('Provisioning succeeded!')

        return True


@command()
def puppet(certname, puppet_master_hostname):
    return 'puppet agent --detailed-exitcodes --color=false --no-daemonize --logdest console --onetime --certname {0} --server {1}'.format(certname, puppet_master_hostname)

@command()
def generate_certificate(certname):
    log.debug('Generating certificate for {0}'.format(certname))
    return 'puppetca generate {0}'.format(certname)




