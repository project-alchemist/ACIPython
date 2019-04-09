class WorkerInfo:

    id = 0
    hostname = ""
    address = ""
    port = 0
    group_id = 0

    def __init__(self, id=0, hostname="0", address="0", port=0, group_id=0):
        self.id = id
        self.hostname = hostname
        self.address = address
        self.port = port
        self.group_id = group_id

        self.connected = False

        self.sock = []

    def to_string(self, space):
        meta = "{0} ID:           {1}\n".format(space, self.id)
        meta += "{0} Hostname:     {1}\n".format(space, self.hostname)
        meta += "{0} Address:      {1}\n".format(space, self.address)
        meta += "{0} Port:         {1}\n".format(space, self.port)
        meta += "{0} Group ID:     {1}\n".format(space, self.group_id)

        return meta
