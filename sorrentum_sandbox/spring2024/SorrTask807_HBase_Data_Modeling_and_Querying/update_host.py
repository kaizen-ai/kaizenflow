import subprocess
import re

def get_container_ip(container_name):
    """ Get the IP address of the Docker container. """
    try:
        cmd = ['docker', 'inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container_name]
        ip_address = subprocess.check_output(cmd).decode('utf-8').strip()
        return ip_address
    except subprocess.CalledProcessError as e:
        print(f"Error obtaining IP address: {e}")
        return None

def update_hosts(ip, hostname):
    """ Update the /etc/hosts file with the new IP address for the hostname. """
    try:
        hosts_path = '/etc/hosts'
        with open(hosts_path, 'r+') as file:
            lines = file.readlines()
            file.seek(0)
            updated = False
            pattern = re.compile(r'\s+' + re.escape(hostname) + r'(\s|$)')
            for line in lines:
                if not pattern.search(line):
                    file.write(line)
                else:
                    file.write(re.sub(r'^\S+', ip, line))
                    updated = True
            if not updated:
                file.write(f"{ip}\t{hostname}\n")
            file.truncate()
        print(f"Updated /etc/hosts to point {hostname} to {ip}")
    except Exception as e:
        print(f"Failed to update /etc/hosts: {e}")

def main():
    container_name = 'hbase-docker'
    hostname = 'hbase-docker'

    # Get the container's IP address
    ip = get_container_ip(container_name)
    if ip:
        print(f"IP Address for {container_name}: {ip}")
        
        # Update /etc/hosts file
        print("Updating /etc/hosts...")
        update_hosts(ip, hostname)
    else:
        print("Failed to obtain IP address.")

if __name__ == '__main__':
    main()

