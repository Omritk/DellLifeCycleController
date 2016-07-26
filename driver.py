import time
import re
import paramiko
from distutils.version import LooseVersion
from cloudshell.shell.core.resource_driver_interface import ResourceDriverInterface
from cloudshell.shell.core.context import InitCommandContext, ResourceCommandContext
from cloudshell.api.cloudshell_api import CloudShellAPISession as cs_api


class DellLifecycleDriver (ResourceDriverInterface):

    def _logger(self, message, path=r'c:\ProgramData\QualiSystems\Dell.log', mode='a'):
        with open(path, mode=mode) as f:
            f.write(message)
        f.close()

    def cleanup(self, chan=None):
        if chan:
            try:
                chan.close()
            except:
                pass
            try:
                chan.keep_this.close()
            except:
                pass

    def __init__(self):
        pass

    def _cs_session(self, context):
        self.cs_api = cs_api
        self.admin_token = context.connectivity.admin_auth_token
        self.server_address = context.connectivity.server_address
        self.session = self.cs_api(self.server_address, token_id=self.admin_token, domain='Global')

    def initialize(self, context):
        """
        Initialize the driver session, this function is called everytime a new instance of the driver is created
        This is a good place to load and cache the driver configuration, initiate sessions etc.
        :param InitCommandContext context: the context the command runs on
        """
        self._cs_session(context=context)
        self.address = context.resource.address
        self.attrs = context.resource.attributes
        self.user = context.resource.attributes["User"]
        self.encrypted = context.resource.attributes["Password"]
        self.password = self.session.DecryptPassword(self.encrypted).Value
        self.name = context.resource.name

    def _session(self):
        # Init Paramiko
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # allow auto-accepting new hosts
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Connecting SSH with; User: " + self.user + " Password: " + self.password + " Address: " + self.address + '''\r\n''')
        try:
            ssh.connect(self.address, 22, username=self.user, password=self.password)
        except Exception, e:
            self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Got error while connecting to: " + self.name + " Error: " + str(e) + '\r\n')
            #self._WriteMessage("Got Error while trying to connect to: " + self.name + " Error: " + str(e))
            raise Exception("Got Exception: " + str(e))
        chan = ssh.invoke_shell()
        chan.keep_this = ssh
        return chan

    def get_running_os(self, context):
        """
        :param ResourceCommandContext context: the context the command runs on
        """
        self.reservationid = context.reservation.reservation_id
        self._cs_session(context=context)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + "Getting OS Info of: " + self.name + '\r\n')
        chan = self._session()
        self._WriteMessage("Getting OS for: " + self.name)
        answer = self._GetOS(chan)
        self._WriteMessage("OS Version: for: " + self.name + " Is: " + answer)
        self._WriteMessage("Done")
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + "OS Info of: " + self.name + " " + answer + '\r\n')
        self.cleanup(chan=chan)

    def get_firmware(self, context, firmware_type):
        """
        :param ResourceCommandContext context: the context the command runs on
        """
        self.reservationid = context.reservation.reservation_id
        self._cs_session(context=context)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + "Getting Firmware Info of " + firmware_type + " for " + self.name + '\r\n')
        chan = self._session()
        if firmware_type.lower() == 'bios':
            self._WriteMessage("Getting " + firmware_type + " Firmware Version for: " + self.name)
            version = self._GetBIOS(chan)
        elif (firmware_type.lower() == 'idrac') or (firmware_type.lower() == 'lifecycle'):
            self._WriteMessage("Getting " + firmware_type + " Firmware Version for: " + self.name)
            version = self._GetFW(chan)
        else:
            self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + "Bad Input for Getting Firmware: " + firmware_type + '\r\n')
            self._WriteMessage("Bad Input: " + firmware_type)
            self.cleanup(chan=chan)
            raise Exception("Bad Input: " + firmware_type)
        self._WriteMessage("Current version is: " + version)
        self._WriteMessage("Done")
        self.cleanup(chan=chan)

    def _WriteMessage(self, message):
        self.session.WriteMessageToReservationOutput(self.reservationid, message)

    def _do_command_and_wait(self, chan, command, expect):
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': ssh : ' + command + ' : wait for : ' + expect + '\r\n')
        chan.send(command + '\n')
        buff = ''
        # while buff.find(expect) < 0:
        while not re.search(expect, buff, 0):
            time.sleep(5)
            resp = chan.recv(9999)
            buff += resp
            # print resp
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': replay : ' + buff + ' : wait for : ' + expect + '\r\n')
        return buff

    def _GetOS(self, chan):
        exp = ">"
        command = "racadm"
        line = "OS Name                 = "
        rst = self._do_command_and_wait(chan, command, exp)
        command = "getsysinfo"
        rst = self._do_command_and_wait(chan, command, exp)
        ans = rst.split(line)[1]
        ans = ans.split('''\r\n''')[0]
        chan.close()
        chan.keep_this.close()
        return ans

    def _GetFW(self, chan):
        exp = ">"
        command = "racadm"
        line = "Firmware Version        = "
        rst = self._do_command_and_wait(chan, command, exp)
        command = "getsysinfo"
        rst = self._do_command_and_wait(chan, command, exp)
        ans = rst.split(line)[1]
        ans = ans.split('''\r\n''')[0]
        chan.close()
        chan.keep_this.close()
        return ans

    def _CheckJobStatus(self, chan, job_id):
        command = 'racadm jobqueue view'
        exp = '>'
        out = self._do_command_and_wait(chan, command, exp)
        rst1 = out.split('Job ID')
        message = rst1
        status = 'Error'
        for job in rst1:
            if job_id in job:
                message = (job.split('Message=')[1]).split('\n')[0]
                status = (job.split('Status=')[1]).split('\n')[0]
                break
        return message, status

    def _GetBIOS(self, chan):
        exp = ">"
        command = "racadm"
        line = "System BIOS Version     = "
        self._do_command_and_wait(chan, command, exp)
        command = "getsysinfo"
        rst = self._do_command_and_wait(chan, command, exp)
        ans = rst.split(line)[1]
        ans = ans.split('''\r\n''')[0]
        chan.close()
        chan.keep_this.close()
        return ans

    def power_control(self, context, operation):
        """
        :param ResourceCommandContext context: the context the command runs on
        """
        self.reservationid = context.reservation.reservation_id
        self._cs_session(context=context)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Doing Power Operation for: ' + self.name + " Command: " + operation + '''\r\n''')
        exp = '>'
        command = 'racadm serveraction '
        if operation.lower() == 'start':
            command += 'powerup'
        elif operation.lower() == 'stop':
            command += 'powerdown'
        elif operation.lower() == 'reboot':
            command += 'powercycle'
        elif operation.lower() == 'hardreset':
            command += 'hardreset'
        elif operation == 'status':
            command += 'powerstatus'
        else:
            self._WriteMessage("Bad Input: " + operation)
            self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Bad Input for Power Operation: ' + operation + '''\r\n''')
            raise Exception("Bad Input: " + operation)
        chan = self._session()
        ans = self._do_command_and_wait(chan, command, exp)
        out = ''
        for line in ans.splitlines():
            if "Server " in line:
                out = line
        self._WriteMessage(out if out else ans)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Answer for Power Operation for: ' + self.name + " Replay: " + ans + '''\r\n''')
        self.cleanup(chan=chan)

    def update_firmware(self, context, firmware_type):
        """
        :param ResourceCommandContext context: the context the command runs on
        """
        self.reservationid = context.reservation.reservation_id
        self._cs_session(context=context)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Updating Firmware For: ' + self.name + " Firmware Type: " + firmware_type + '''\r\n''')
        chan = self._session()
        if firmware_type.lower() == 'bios':
            self._WriteMessage("Going to update BIOS version.")
            version = self._GetBIOS(chan)
            file_name = 'bios.EXE'
        elif (firmware_type.lower() == 'idrac') or (firmware_type.lower() == 'lifecycle'):
            self._WriteMessage("Going to update iDRAC & LifeCycle Controller version.")
            version = self._GetFW(chan)
            file_name = 'idrac.EXE'
        else:
            self.cleanup(chan=chan)
            self._WriteMessage("Bad Input: " + str(firmware_type))
            self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Bad Input for updating Firmware For: ' + self.name + " Firmware Type: " + firmware_type + '''\r\n''')
            raise Exception("Bad Input: " + firmware_type)

        self._WriteMessage("Current FW Version is: " + version)
        command = 'racadm set lifecyclecontroller.lcattributes.lifecyclecontrollerstate 1'
        exp = '>'
        chan = self._session()
        self._do_command_and_wait(chan, command, exp)
        remote_server = '192.168.42.207'
        remote_folder = 'Dell'
        ftp_user = 'User'
        ftp_password = 'Aa123456'
        combine_path = '//' + remote_server + '/' + remote_folder
        command = 'racadm update -f {filename} -u {username} -p {password} -l {path}'.format(filename=file_name, username=ftp_user, password=ftp_password, path=combine_path)
        self._do_command_and_wait(chan, command, exp)
        command = 'racadm jobqueue view'
        out = self._do_command_and_wait(chan, command, exp)
        # if ('ERROR: RAC991' in out) or ('ERROR: RAC1135' in out):
        #     return out, 'Error'
        rst1 = out.split('Job ID')
        job_id = ''
        message = ''
        for job in rst1:
            if 'Downloading' in job and 'Firmware Update' in job:
                job_id = (job.split('JID')[1]).split('\n')[0]
                message = (job.split('Message=')[1]).split('\n')[0]

        if not job_id:
            self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + "Couldn't get Job ID: " + out + '''\r\n''')
            self._WriteMessage("Couldn't get Job ID: " + message)
            raise Exception("Couldn't get Job ID: " + message)
        done = True
        try:
            while done:
                msg, stat = self._CheckJobStatus(chan, job_id)
                if 'Failed' in stat:
                    self._WriteMessage("Failed to Update Firmware: " + msg)
                    self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Failed to Updating the Firmware: " + msg + '''\r\n''')
                    raise Exception("Failed to Update Firmware: " + msg)

                elif 'Error' in stat:
                    self._WriteMessage("Got Error running Update Firmware: " + msg)
                    self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Got Error Updating the Firmware: " + msg + '''\r\n''')
                    raise Exception("Got Error running Update Firmware: " + msg)

                elif 'scheduled' in msg.lower():
                    self._WriteMessage(msg)
                    self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Updating the Firmware is Scheduled, Rebooting: " + msg + '''\r\n''')
                    self._WriteMessage("Rebooting Server...")
                    self.cleanup(chan=chan)
                    self.power_control(context, 'reboot')
                    time.sleep(30)
                    done = False
                elif 'Completed' in stat:
                    self._WriteMessage(msg)
                    self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Updating the Firmware is Completed: " + msg + '''\r\n''')
                    done = False
                else:
                    self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Updating Firmware Status: " + msg + '''\r\n''')
                    self._WriteMessage(msg)
                time.sleep(5)
            self.cleanup(chan=chan)
            self._VerifyFirmware(version, firmware_type, job_id)

        except Exception, e:
            self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + "Got Exception Running command: (Could be false-positive due to iDRAC resetting it self)" + str(e) + '''\r\n''')
            self.cleanup(chan=chan)
            time.sleep(30)
            self._VerifyFirmware(version, firmware_type, job_id)
        finally:
            self.cleanup(chan=chan)

    def _VerifyFirmware(self, ver, fw_type, jid):
        retires = 10
        delay = 20
        chan = ''
        self._WriteMessage("Trying to reconnect to the iDRAC (Might take up-to 3 minutes)")
        for x in xrange(retires):
            try:
                self._logger("Trying to re-connect to \"" + self.name + '\"... Retry number: ' + str(x) + '\r\n')
                chan = self._session()
                break
            except Exception, e:
                self._logger("Got Error: " + str(e) + '''\r\n''')
                self._logger("Failed to connect to \"" + self.name + '\"... Retrying in: ' + str(delay) + " Seconds..." + '\r\n')
                time.sleep(delay)

        if not chan:
            self._WriteMessage("Failed to connect to \"" + self.name + '\" after ' + str(retires) + ' times')
            self._logger("Failed to connect to \"" + self.name + '\" after ' + str(retires) + ' times' + '\r\n')
            raise Exception("Failed to connect to \"" + self.name + '\" after ' + str(retires) + ' times')
        new_ver = ''
        status_retries = 100
        status_delay = 5
        if fw_type.lower() == 'bios':
            for x in xrange(status_retries):
                msg, stat = self._CheckJobStatus(chan, jid)
                if 'Failed' in stat:
                    self._WriteMessage("Failed to Update Firmware: " + msg)
                    self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Failed to Update the Firmware: " + msg + '''\r\n''')
                    raise Exception("Failed to Update Firmware: " + msg)

                elif 'Error' in stat:
                    self._WriteMessage("Got Error running Update Firmware: " + msg)
                    self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Got Error Updating the Firmware: " + msg + '''\r\n''')
                    raise Exception("Got Error running Update Firmware: " + msg)

                elif 'Completed' in stat:
                    self._WriteMessage(msg)
                    self._logger("Job Completed: " + msg)
                    break
                else:
                    self._WriteMessage(msg)
                    self._logger("Job Status: " + msg)
                    time.sleep(status_delay)
            try:
                new_ver = self._GetBIOS(chan)
            except Exception, e:
                self._logger("Got Error while trying to query for FW Version: " + str(e) + ' retrying..' + '\r\n', )
                new_ver = self._GetBIOS(chan)
        elif (fw_type.lower() == 'idrac') or (fw_type.lower() == 'lifecycle'):
            try:
                new_ver = self._GetFW(chan)
            except Exception, e:
                self._logger("Got Error while trying to query for FW Version: " + str(e) + ' retrying..' + '\r\n', )
                new_ver = self._GetFW(chan)
        if LooseVersion(new_ver) > LooseVersion(ver):
            out = "Successfully updated the " + fw_type + ' Firmware to version: ' + new_ver
        elif LooseVersion(new_ver) == LooseVersion(ver):
            out = "Version stayed the same(" + new_ver + "), please check the logs if it wasn't intended"
        else:
            out = "New Version is: " + new_ver

        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + " Updating Firmware completed: " + out + '''\r\n''')
        self._WriteMessage(out)
        self._WriteMessage("Done Updating Firmware")

    def _get_v_disks(self, chan):
        command = 'racadm raid get vdisks -o'
        exp = ">"
        out = self._do_command_and_wait(chan, command, exp)
        rst = out.split('Disk.Virtual.')
        disk_names = []
        disk_sizes = []
        disk_raids = []
        for disk in rst:
            if (disk != '\n') and ('racadm' not in disk):
                disk_name = disk.split('Name                             = ')[1].split('\r\n')[0]
                disk_size = disk.split('Size                             = ')[1].split('\r\n')[0]
                disk_raid = disk.split('Layout                           = ')[1].split('\r\n')[0]
                disk_names.append(disk_name)
                disk_sizes.append(disk_size)
                disk_raids.append(disk_raid)
        return disk_names, disk_sizes, disk_raids

    def _get_p_disks(self, chan):
        command = 'racadm raid get pdisks -o'
        exp = ">"
        out = self._do_command_and_wait(chan, command, exp)
        rst = out.split('Disk.Bay.')
        disk_names = []
        disk_sizes = []
        for disk in rst:
            if (disk != '\n') and ('racadm' not in disk):
                disk_name = disk.split('Name                             = ')[1].split('\r\n')[0]
                disk_size = disk.split('Size                             = ')[1].split('\r\n')[0]
                disk_names.append(disk_name)
                disk_sizes.append(disk_size)
        return disk_names, disk_sizes

    def get_disks(self, context):
        """
        :param ResourceCommandContext context: the context the command runs on
        """
        self.reservationid = context.reservation.reservation_id
        self._cs_session(context=context)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Getting Virtual & Physical disks For: ' + self.name + '''\r\n''')
        chan = self._session()
        v_ds_name, v_ds_size, v_ds_raid = self._get_v_disks(chan)
        p_ds_name, p_ds_size = self._get_p_disks(chan)
        self.cleanup(chan=chan)
        out = ''
        if len(v_ds_name) > 0:
            out += "Found " + str(len(v_ds_name)) + " Virtual Disks" + '\n'
            for x in xrange(len(v_ds_name)):
                out += "Disk " + str(x) + ": Name:" + v_ds_name[x].strip() + " Size: " + v_ds_size[x].strip() + " Raid Config: " + v_ds_raid[x].strip() + '\n'
        else:
            out += "Couldn't find any Virtual Disks" + '\n'

        if len(p_ds_name) > 0:
            out += "Found " + str(len(p_ds_name)) + " Physical Disks" + '\n'
            for x in xrange(len(p_ds_name)):
                out += "Disk " + str(x) + ": Name:" + p_ds_name[x].strip() + " Size: " + p_ds_size[x].strip() + '\n'
        else:
            out += "Couldn't find any Physical Disks" + '\n'
        if out != '':
            self._WriteMessage(out)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Getting Virtual & Physical disks For: ' + self.name + " Output: " + out + '''\r\n''')

    def change_root_password(self, context, password):
        """
        :param ResourceCommandContext context: the context the command runs on
        """
        self.reservationid = context.reservation.reservation_id
        self._cs_session(context=context)
        self._logger(time.strftime('%Y-%m-%d %H:%M:%S') + ': Changing root password for: ' + self.name + '''\r\n''')
        self._WriteMessage("Going to change root password")
        chan = self._session()
        command = 'racadm set iDRAC.Users.2.Password ' + password
        exp = '>'
        out = self._do_command_and_wait(chan, command, exp)
        try:
            if 'successfully' in out:
                self._WriteMessage("Successfully change password to: " + password)
                self._logger("Successfully change password to: " + password + '\r\n')
                self.session.SetAttributeValue(self.name, 'Password', password)
                self.password = password
            else:
                self._logger("Failed to change password for " + self.name + ", Error: " + out + '\r\n')
                self._WriteMessage("Failed to change password for " + self.name + ', Error: ' + out)
        except Exception, e:
            self._logger("Failed to change password for " + self.name + ", Error: " + str(e) + '\r\n')
            self._WriteMessage("Failed to change password for " + self.name + ', Error: ' + str(e))

        finally:
            self.cleanup(chan=chan)