import 'dart:math';

import 'package:dio/dio.dart';

import '../../core/utils/file_utils.dart';

/// Result of analyzing a URL before downloading.
class UrlAnalysis {
  final int contentLength;
  final bool supportsRange;
  final String? suggestedFileName;
  final String? mimeType;
  final String? resolvedUrl; // Final URL after redirects
  final Map<String, String> responseHeaders;

  const UrlAnalysis({
    required this.contentLength,
    required this.supportsRange,
    this.suggestedFileName,
    this.mimeType,
    this.resolvedUrl,
    this.responseHeaders = const {},
  });
}

/// Represents a byte-range segment for downloading.
class SegmentInfo {
  final int index;
  final int startByte;
  final int endByte;

  const SegmentInfo({
    required this.index,
    required this.startByte,
    required this.endByte,
  });

  int get totalBytes => endByte - startByte + 1;

  SegmentInfo copyWith({int? index, int? startByte, int? endByte}) =>
      SegmentInfo(
        index: index ?? this.index,
        startByte: startByte ?? this.startByte,
        endByte: endByte ?? this.endByte,
      );
}

class SegmentManager {
  final Dio _dio;

  SegmentManager(this._dio);

  /// Analyze URL to determine file info, range support, and final URL after redirects.
  /// First resolves all redirects manually, then probes the final URL.
  Future<UrlAnalysis> analyzeUrl(
    String url, {
    Map<String, String> headers = const {},
    int timeoutSeconds = 30,
  }) async {
    // Step 1: Resolve all redirects manually to find the real download URL
    final resolvedUrl = await _resolveRedirects(url, headers: headers, timeoutSeconds: timeoutSeconds);

    // Step 2: Try HEAD on the resolved URL
    try {
      final response = await _dio.head<void>(
        resolvedUrl,
        options: Options(
          headers: headers,
          followRedirects: false, // Already resolved
          receiveTimeout: Duration(seconds: timeoutSeconds),
          sendTimeout: Duration(seconds: timeoutSeconds),
        ),
      );

      final responseHeaders = <String, String>{};
      response.headers.forEach((name, values) {
        responseHeaders[name] = values.join(', ');
      });

      return UrlAnalysis(
        contentLength: _parseContentLength(response.headers),
        supportsRange: _checkRangeSupport(response.headers),
        suggestedFileName: _extractFileName(response.headers, resolvedUrl),
        mimeType: response.headers.value('content-type'),
        resolvedUrl: resolvedUrl,
        responseHeaders: responseHeaders,
      );
    } catch (_) {
      // HEAD failed — try GET probe on resolved URL
      return _probeWithGet(resolvedUrl, headers: headers, timeoutSeconds: timeoutSeconds);
    }
  }

  /// Manually follow all redirects (301/302/303/307/308) up to 30 hops.
  /// Uses HEAD requests to avoid downloading file content during resolution.
  /// Falls back to GET with immediate cancellation if HEAD fails.
  Future<String> _resolveRedirects(
    String url, {
    Map<String, String> headers = const {},
    int timeoutSeconds = 30,
    int maxHops = 30,
  }) async {
    var currentUrl = url;

    for (var i = 0; i < maxHops; i++) {
      try {
        // Try HEAD first — no body to download
        final response = await _headForRedirect(
          currentUrl,
          headers: headers,
          timeoutSeconds: timeoutSeconds,
        );

        final statusCode = response.statusCode ?? 200;

        // Check if it's a redirect
        if (_isRedirect(statusCode)) {
          final location = response.headers.value('location');
          if (location == null || location.isEmpty) break;

          // Resolve relative URLs
          final baseUri = Uri.parse(currentUrl);
          currentUrl = baseUri.resolve(location).toString();
          continue;
        }

        // Not a redirect — check realUri (Dio might have followed some)
        if (response.realUri.toString() != currentUrl) {
          currentUrl = response.realUri.toString();
        }

        break; // Final URL found
      } on DioException catch (e) {
        // If we get a redirect in the error, follow it
        if (e.response != null) {
          final statusCode = e.response!.statusCode ?? 0;
          if (_isRedirect(statusCode)) {
            final location = e.response!.headers.value('location');
            if (location != null && location.isNotEmpty) {
              final baseUri = Uri.parse(currentUrl);
              currentUrl = baseUri.resolve(location).toString();
              continue;
            }
          }
        }
        // HEAD failed entirely — try GET with cancel token as fallback
        final resolved = await _getForRedirectFallback(
          currentUrl,
          headers: headers,
          timeoutSeconds: timeoutSeconds,
        );
        if (resolved != null && resolved != currentUrl) {
          currentUrl = resolved;
          continue;
        }
        break; // Can't resolve further
      } catch (_) {
        break;
      }
    }

    return currentUrl;
  }

  /// HEAD request for redirect resolution — no body downloaded.
  Future<Response<void>> _headForRedirect(
    String url, {
    Map<String, String> headers = const {},
    int timeoutSeconds = 30,
  }) {
    return _dio.head<void>(
      url,
      options: Options(
        headers: headers,
        followRedirects: false,
        validateStatus: (status) =>
            status != null && (status < 400 || _isRedirect(status)),
        receiveTimeout: Duration(seconds: timeoutSeconds),
        sendTimeout: Duration(seconds: timeoutSeconds),
      ),
    );
  }

  /// GET fallback for servers that reject HEAD. Uses CancelToken to abort
  /// immediately after reading headers — never downloads the body.
  Future<String?> _getForRedirectFallback(
    String url, {
    Map<String, String> headers = const {},
    int timeoutSeconds = 30,
  }) async {
    final cancelToken = CancelToken();
    try {
      final response = await _dio.get<void>(
        url,
        cancelToken: cancelToken,
        options: Options(
          headers: headers,
          followRedirects: false,
          validateStatus: (status) =>
              status != null && (status < 400 || _isRedirect(status)),
          receiveTimeout: Duration(seconds: timeoutSeconds),
          sendTimeout: Duration(seconds: timeoutSeconds),
          responseType: ResponseType.stream,
        ),
      );

      final statusCode = response.statusCode ?? 200;

      // Cancel immediately — we only need headers
      cancelToken.cancel('headers received');

      if (_isRedirect(statusCode)) {
        final location = response.headers.value('location');
        if (location != null && location.isNotEmpty) {
          final baseUri = Uri.parse(url);
          return baseUri.resolve(location).toString();
        }
      }

      // Check if Dio followed a redirect
      final realUri = response.realUri.toString();
      if (realUri != url) return realUri;

      return url;
    } on DioException catch (e) {
      // Cancelled is expected — check if we got redirect info
      if (e.response != null && _isRedirect(e.response!.statusCode ?? 0)) {
        final location = e.response!.headers.value('location');
        if (location != null && location.isNotEmpty) {
          final baseUri = Uri.parse(url);
          return baseUri.resolve(location).toString();
        }
      }
      return null;
    } catch (_) {
      return null;
    }
  }

  bool _isRedirect(int statusCode) =>
      statusCode == 301 ||
      statusCode == 302 ||
      statusCode == 303 ||
      statusCode == 307 ||
      statusCode == 308;

  /// Probe URL with a GET range request if HEAD is not supported.
  Future<UrlAnalysis> _probeWithGet(
    String url, {
    Map<String, String> headers = const {},
    int timeoutSeconds = 30,
  }) async {
    final probeHeaders = Map<String, String>.from(headers);
    probeHeaders['Range'] = 'bytes=0-0';

    final response = await _dio.get<void>(
      url,
      options: Options(
        headers: probeHeaders,
        followRedirects: true,
        maxRedirects: 10,
        receiveTimeout: Duration(seconds: timeoutSeconds),
        sendTimeout: Duration(seconds: timeoutSeconds),
        // Don't download the body
        responseType: ResponseType.stream,
      ),
    );

    // Close the stream immediately
    final stream = response.data as ResponseBody?;
    await stream?.stream.drain<void>();

    final responseHeaders = <String, String>{};
    response.headers.forEach((name, values) {
      responseHeaders[name] = values.join(', ');
    });

    final supportsRange = response.statusCode == 206;
    var contentLength = -1;

    if (supportsRange) {
      // Parse Content-Range: bytes 0-0/total
      final contentRange = response.headers.value('content-range');
      if (contentRange != null) {
        final match = RegExp(r'bytes\s+\d+-\d+/(\d+)').firstMatch(contentRange);
        if (match != null) {
          contentLength = int.parse(match.group(1)!);
        }
      }
    } else {
      contentLength = _parseContentLength(response.headers);
    }

    final resolvedUrl = response.realUri.toString();

    return UrlAnalysis(
      contentLength: contentLength,
      supportsRange: supportsRange,
      suggestedFileName: _extractFileName(response.headers, resolvedUrl),
      mimeType: response.headers.value('content-type'),
      resolvedUrl: resolvedUrl,
      responseHeaders: responseHeaders,
    );
  }

  /// Create N segments for a file of the given total size.
  /// If totalSize is unknown (-1) or range not supported, returns a single segment.
  List<SegmentInfo> createSegments(int totalSize, int threadCount) {
    if (totalSize <= 0 || threadCount <= 1) {
      return [
        SegmentInfo(
          index: 0,
          startByte: 0,
          endByte: totalSize > 0 ? totalSize - 1 : -1,
        ),
      ];
    }

    // Don't create segments smaller than 256KB
    const minSegmentSize = 256 * 1024;
    final effectiveThreads = min(
      threadCount,
      max(1, totalSize ~/ minSegmentSize),
    );

    final segmentSize = totalSize ~/ effectiveThreads;
    final segments = <SegmentInfo>[];

    for (var i = 0; i < effectiveThreads; i++) {
      final start = i * segmentSize;
      final end = (i == effectiveThreads - 1) ? totalSize - 1 : (i + 1) * segmentSize - 1;
      segments.add(SegmentInfo(index: i, startByte: start, endByte: end));
    }

    return segments;
  }


  int _parseContentLength(Headers headers) {
    final cl = headers.value('content-length');
    if (cl == null) return -1;
    return int.tryParse(cl) ?? -1;
  }

  bool _checkRangeSupport(Headers headers) {
    final acceptRanges = headers.value('accept-ranges');
    return acceptRanges != null && acceptRanges.toLowerCase() != 'none';
  }

  String? _extractFileName(Headers headers, String url) {
    // Try Content-Disposition first
    final disposition = headers.value('content-disposition');
    if (disposition != null) {
      // Try filename*=UTF-8''encoded_name
      final starMatch = RegExp(r"filename\*\s*=\s*UTF-8''(.+?)(?:;|$)", caseSensitive: false)
          .firstMatch(disposition);
      if (starMatch != null) {
        return FileUtils.sanitizeFileName(Uri.decodeFull(starMatch.group(1)!.trim()));
      }

      // Try filename="name" or filename=name
      final match = RegExp(r'filename\s*=\s*"?([^";\n]+)"?', caseSensitive: false)
          .firstMatch(disposition);
      if (match != null) {
        return FileUtils.sanitizeFileName(match.group(1)!.trim());
      }
    }

    // Fall back to URL
    return FileUtils.sanitizeFileName(FileUtils.getFileNameFromUrl(url));
  }
}
