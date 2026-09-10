// Fails when line coverage of lib/src falls below a threshold.
//
//     dart run coverage:test_with_coverage
//     dart run tool/check_coverage.dart [minPercent] [lcovPath]
import 'dart:io';

void main(List<String> args) {
  final minimum = args.isNotEmpty ? double.parse(args[0]) : 90.0;
  final path = args.length > 1 ? args[1] : 'coverage/lcov.info';
  final file = File(path);
  if (!file.existsSync()) {
    stderr.writeln(
      'No coverage data at $path. '
      'Run: dart run coverage:test_with_coverage',
    );
    exit(2);
  }

  var hit = 0;
  var total = 0;
  var inScope = false;
  for (final line in file.readAsLinesSync()) {
    if (line.startsWith('SF:')) {
      inScope = line.replaceAll(r'\', '/').contains('lib/src/');
    } else if (inScope && line.startsWith('DA:')) {
      total++;
      if (int.parse(line.substring(3).split(',')[1]) > 0) hit++;
    }
  }
  if (total == 0) {
    stderr.writeln('No lib/src lines found in $path');
    exit(2);
  }

  final percent = 100 * hit / total;
  stdout.writeln(
    'lib/src line coverage: ${percent.toStringAsFixed(1)}% '
    '($hit/$total lines, minimum $minimum%)',
  );
  if (percent < minimum) exit(1);
}
