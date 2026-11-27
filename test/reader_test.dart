import 'package:libserialport/libserialport.dart';
import 'package:test/test.dart';

void main() {
  final port = SerialPort.fromAddress(0);

  group('reader timeout', () {
    for (final timeout in [
      -1,
      0,
      0xffffffff,
      0x100000000,
      0x100000001,
      0x200000000,
      0x7fffffffffffffff,
    ]) {
      test('rejects $timeout before starting native I/O', () {
        expect(
          () => SerialPortReader(port, timeout: timeout),
          throwsArgumentError,
        );
      });
    }

    for (final timeout in [null, 1, 500, 0xfffffffd, 0xfffffffe]) {
      test('accepts ${timeout ?? 'the default'}', () async {
        final reader = SerialPortReader(port, timeout: timeout);
        expect(reader.port, same(port));
        await reader.close();
      });
    }
  });
}
