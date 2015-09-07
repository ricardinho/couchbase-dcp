import struct
from collections import namedtuple, Counter
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator, Protocol
import constants as C

Message = namedtuple('Message', ['magic', 'opcode', 'keylen', 'extlen', 'datatype', 'vbucket',
                                 'total_length', 'opaque', 'cas'])


class DcpProtocol(Protocol):
    prepended_message = b''
    size = 0

    def __init__(self, vbucket, credentials):
        self.vbucket = vbucket
        self.counter = Counter()
        self.username = credentials[0]
        self.password = credentials[1]

    def connect(self):
        extras = struct.pack('>II', 0, C.FLAG_OPEN_PRODUCER)
        req = pack_request(C.CMD_OPEN, 0, self.vbucket, 0,
                           'please give me your data', '', extras)
        print 'sent connection request'
        self.write(req)
        return self

    def work(self):
        self.write(pack_request(*stream(self.vbucket)))
        return self

    def authenticate(self):
        command = C.CMD_SASL_AUTH
        val = '\0'.join(['', self.username, self.password])
        req = pack_request(command, 0, 0, 0, 'PLAIN', val, '')
        self.write(req)
        print 'sent auth request'
        return self

    def write(self, data):
        self.transport.write(data)

    def dataReceived(self, data):
        data = self.prepended_message + data
        datalen = len(data)
        base_index = 0
        magic_chars = map(chr, (C.REQ_MAGIC, C.RES_MAGIC))
        while base_index + C.HEADER_LEN < datalen:
            if data[base_index] in magic_chars:
                try:
                    header = unpack_message(data[base_index: base_index + C.HEADER_LEN])
                    new_index = base_index + C.HEADER_LEN + header.total_length
                    self.handle_message(header, data[base_index:new_index])
                    self.size += new_index - base_index
                    base_index = new_index
                except Exception as e:
                    import pdb; pdb.set_trace()
            else:
                base_index += 1

        self.prepended_message = data[base_index:]

    def failover_request(self):
        req = pack_request(C.CMD_GET_FAILOVER_LOG,
                           0, self.vbucket, 0, '', '', '')
        self.write(req)
        print 'sent failover request'
        return self

    def buffer_ack(self):
        req = pack_request(C.CMD_BUFFER_ACK, 0, self.vbucket, 0 , '', '',
                           struct.pack('>I', self.size))
        self.write(req)

    def close_stream(self, vbucket):
        req = pack_request(C.CMD_CLOSE_STREAM, 0, vbucket, 0 , '', '', '')
        print 'close stream'
        self.write(req)
        return self

    def handle_message(self, header, message):
        try:
            print header, len(message)
            self.counter[header.opcode] += 1
            if header.opcode == C.CMD_GET_FAILOVER_LOG:
                import pdb; pdb.set_trace()
                vbuuid, start = struct.unpack('>QQ', message[24:])
                req = pack_request(*stream(self.vbucket, from_number=start, vb_uuid=vbuuid))
                self.write(req)
            if header.opcode == C.CMD_STREAM_END:
                import pdb; pdb.set_trace()
        except Exception as e:
            import pdb; pdb.set_trace()

opaque = 0xFFFF0000


def unpack_message(message):
    mess = struct.unpack(C.PKT_HEADER_FMT, message)
    return Message(*mess)


def pack_request(opcode, data_type, vbucket, cas, key, value, extras):
    global opaque
    opaque += 1
    res = struct.pack(C.PKT_HEADER_FMT, C.REQ_MAGIC,
                      opcode, len(key), len(extras),
                      data_type, vbucket, len(key) + len(value) + len(extras),
                      opaque, cas) + extras + key + value
    return res



def stream(vbucket, from_number=0, to_number=999999, vb_uuid=0):
    extras = struct.pack(">IIQQQQQ", 0x04, 0, from_number, to_number,
                         vb_uuid, from_number, to_number)
    return C.CMD_STREAM_REQ, 0, vbucket, 0, '', '', extras


def err(ex, *args, **kwargs):
    import pdb; pdb.set_trace()


def get_vbucket_data(host, port, vbucket, credentials):
    cli = ClientCreator(reactor, DcpProtocol, vbucket, credentials)
    conn = cli.connectTCP(host, port)
    conn.addCallback(lambda x: x.authenticate()) \
        .addCallback(lambda x: x.connect()) \
        .addCallback(lambda x: x.failover_request())
    reactor.run()
