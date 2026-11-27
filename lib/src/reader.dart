/*
 * Based on libserialport (https://sigrok.org/wiki/Libserialport).
 *
 * Copyright (C) 2010-2012 Bert Vermeulen <bert@biot.com>
 * Copyright (C) 2010-2015 Uwe Hermann <uwe@hermann-uwe.de>
 * Copyright (C) 2013-2015 Martin Ling <martin-libserialport@earth.li>
 * Copyright (C) 2013 Matthias Heidbrink <m-sigrok@heidbrink.biz>
 * Copyright (C) 2014 Aurelien Jacobs <aurel@gnuage.org>
 * Copyright (C) 2020 J-P Nurmi <jpnurmi@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import 'dart:async';
import 'dart:ffi' as ffi;
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart' as ffi;
import 'package:libserialport/src/bindings.dart';
import 'package:libserialport/src/dylib.dart';
import 'package:libserialport/src/error.dart';
import 'package:libserialport/src/port.dart';

/// Asynchronous serial port reader.
///
/// Provides a [stream] that can be listened to asynchronously to receive data
/// whenever available.
///
/// The [stream] will attempt to open a given [port] for reading. If the stream
/// fails to open the port, it will emit [SerialPortError]. If the port is
/// successfully opened, the stream will begin emitting [Uint8List] data events.
///
/// **Note:** The reader must be closed using [close()] when done with reading.
abstract class SerialPortReader {
  /// Creates a reader for the port. Optional [timeout] parameter can be
  /// provided to bound each blocking read in milliseconds. It must be in
  /// 1..0xfffffffe so cancellation can wait for the worker to finish. Zero
  /// disables the native timeout; Windows reserves 0xffffffff. Defaults to 500ms.
  factory SerialPortReader(SerialPort port, {int? timeout}) =>
      _SerialPortReaderImpl(port, timeout: timeout);

  /// Gets the port the reader operates on.
  SerialPort get port;

  /// Gets a stream of data.
  Stream<Uint8List> get stream;

  /// Closes the stream after the reader has finished using the port.
  Future<void> close();
}

class _SerialPortReaderArgs {
  final int address;
  final int timeout;
  final SendPort sendPort;
  _SerialPortReaderArgs({
    required this.address,
    required this.timeout,
    required this.sendPort,
  });
}

class _SerialPortReaderImpl implements SerialPortReader {
  final SerialPort _port;
  final int _timeout;
  Future<void> _operations = Future<void>.value();
  Future<void> Function()? _stopWorker;
  Future<void>? _closing;
  bool _closed = false;
  static const MAX_LEN = 255;

  _SerialPortReaderImpl(SerialPort port, {int? timeout})
      : _port = port,
        _timeout = timeout ?? 500 {
    RangeError.checkValueInInterval(_timeout, 1, 0xfffffffe, 'timeout');
  }

  @override
  SerialPort get port => _port;

  @override
  Stream<Uint8List> get stream => _controller.stream;

  late final _controller = StreamController<Uint8List>(
    onListen: _start,
    onCancel: _cancel,
    onPause: _cancel,
    onResume: _start,
  );

  @override
  Future<void> close() {
    _closed = true;
    return _closing ??= _cancel().whenComplete(() {
      _controller.close();
    });
  }

  void _start() {
    _operations = _operations.then((_) => _startRead());
  }

  Future<void> _cancel() {
    return _operations = _operations.then((_) async {
      final stop = _stopWorker;
      _stopWorker = null;
      await stop?.call();
    });
  }

  Future<void> _startRead() async {
    if (_closed ||
        !_controller.hasListener ||
        _controller.isPaused ||
        _stopWorker != null) {
      return;
    }
    final receiver = ReceivePort();
    final ready = Completer<SendPort>();
    ready.future.ignore();
    final exited = Completer<void>();
    var stopping = false;
    receiver.listen((message) {
      if (message is SendPort) {
        ready.complete(message);
      } else if (message == null) {
        receiver.close();
        if (!ready.isCompleted) {
          ready.completeError(
            StateError('Serial reader exited during startup'),
          );
        }
        exited.complete();
        if (!stopping && !_controller.isClosed) _controller.close();
      } else if (!_closed && !_controller.isClosed) {
        if (message is Uint8List) {
          _controller.add(message);
        } else if (message is SerialPortError) {
          _controller.addError(message);
        } else if (message is List) {
          _controller.addError(
            StateError('${message[0]}'),
            StackTrace.fromString('${message[1]}'),
          );
        }
      }
    });
    try {
      await Isolate.spawn(
        _waitRead,
        _SerialPortReaderArgs(
          address: _port.address,
          timeout: _timeout,
          sendPort: receiver.sendPort,
        ),
        debugName: toString(),
        onExit: receiver.sendPort,
        onError: receiver.sendPort,
      );
      final commands = await ready.future;
      _stopWorker = () async {
        stopping = true;
        if (!exited.isCompleted) commands.send(null);
        await exited.future;
      };
    } catch (error, stack) {
      receiver.close();
      if (!_controller.isClosed) {
        _controller.addError(error, stack);
        _controller.close();
      }
    }
  }

  static Future<void> _waitRead(_SerialPortReaderArgs args) async {
    final port = ffi.Pointer<sp_port>.fromAddress(args.address);
    final commands = ReceivePort();
    var stopping = false;
    commands.listen((_) => stopping = true);
    args.sendPort.send(commands.sendPort);
    try {
      while (!stopping) {
        final bytes = ffi.using((arena) {
          final ptr = arena<ffi.Uint8>(MAX_LEN);
          final count = dylib.sp_blocking_read_next(
            port,
            ptr.cast(),
            MAX_LEN,
            args.timeout,
          );
          if (count > 0) {
            args.sendPort.send(Uint8List.fromList(ptr.asTypedList(count)));
          }
          return count;
        });
        if (bytes < 0) {
          args.sendPort.send(
            SerialPort.lastError ??
                SerialPortError('Serial read failed', bytes),
          );
          break;
        }
        await Future<void>.delayed(Duration.zero);
      }
    } finally {
      commands.close();
    }
  }
}
