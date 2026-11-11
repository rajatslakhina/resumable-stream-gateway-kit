#!/usr/bin/env bash
# Each mutant re-introduces a specific bug the README claims is impossible.
# A mutant "survives" if the test suite still passes with it applied: that
# means the claim is untested. The script fails if any mutant survives.
set -u
cd "$(dirname "$0")/.."
S=Sources/StreamGateway
# file | exact text to replace (must occur exactly once) | replacement | the claim it breaks
MUTANTS=(
  "$S/TranscriptAssembler.swift|guard cursor.sequence == expected else|guard cursor.sequence >= expected else|client refuses gaps"
  "$S/TranscriptAssembler.swift|if cursor.sequence <= lastSequence {|if cursor.sequence < lastSequence {|client drops duplicates"
  "$S/StreamGateway.swift|frames: .init(backlog: backlog,|frames: .init(backlog: [],|resume replays the missed frames"
  "$S/StreamGateway.swift|bufferingPolicy: .bufferingOldest(configuration.subscriberQueueLimit))|bufferingPolicy: .bufferingNewest(configuration.subscriberQueueLimit))|slow consumers are cut, never silently skipped"
  "$S/StreamGateway.swift|guard cursor.epoch == state.log.epoch else { throw reject(.cursorMismatch) }|_ = cursor.epoch|cursors from another generation are rejected"
  "$S/StreamGateway.swift|?? \"upstream closed without a stop reason\"
        finish(state, kind: .error, payload: message)|?? \"upstream closed without a stop reason\"
        finish(state, kind: .done, payload: message)|truncation is never reported as done"
  "$S/StreamGateway.swift|now - since >= configuration.abandonTimeout|now - since > configuration.abandonTimeout|abandonment fires exactly at the timeout"
  "$S/ReplayLog.swift|            if head != 0 {|            if false {|ring growth keeps frames in order"
  "$S/ReplayLog.swift|if after < newestSequence, after + 1 < oldest { return .gap(oldestAvailable: oldest) }|_ = oldest|evicted frames produce a gap, not a hole"
  "$S/ResumableStreamClient.swift|            if progressed {|            if false {|progress resets the failure budget"
  "$S/SSEParser.swift|if byte == Self.lf { return }|_ = byte|CRLF is one line ending"
  "$S/ReconnectPolicy.swift|let floorMs = max(baseMs, hintMs)|let floorMs = baseMs|the server retry hint is a floor"
)
# A mutant that makes a test hang counts as caught, not as a stuck job.
LIMIT=""; command -v timeout >/dev/null && LIMIT="timeout 600"
if ! swift build --build-tests -q >/dev/null 2>&1 || ! $LIMIT swift test --skip-build >/dev/null 2>&1; then
  echo "BASELINE FAILED: the unmutated suite must pass before mutants mean anything"; exit 2
fi
echo "baseline: unmutated suite passes"
survived=0; caught=0
for entry in "${MUTANTS[@]}"; do
  IFS='|' read -r file from to claim <<< "$(printf '%s' "$entry" | tr '\n' '\001')"
  file=${file//$'\001'/$'\n'}; from=${from//$'\001'/$'\n'}; to=${to//$'\001'/$'\n'}; claim=${claim//$'\001'/$'\n'}
  cp "$file" "$file.orig"
  count=$(FROM="$from" perl -0777 -ne '$f=$ENV{FROM}; $c = () = /\Q$f\E/g; print $c' "$file")
  if [ "$count" != "1" ]; then echo "SETUP ERROR: pattern occurs $count times in $file: $claim"; rm "$file.orig"; exit 2; fi
  FROM="$from" TO="$to" perl -0777 -pi -e 'BEGIN { $f = $ENV{FROM}; $t = $ENV{TO} } s/\Q$f\E/$t/' "$file"
  if ! swift build --build-tests -q >/dev/null 2>&1; then
    # A mutant that doesn't compile proves nothing about the tests.
    echo "SETUP ERROR: mutant does not compile: $claim"; mv "$file.orig" "$file"; exit 2
  fi
  if $LIMIT swift test --skip-build >/dev/null 2>&1; then
    echo "SURVIVED: $claim"; survived=$((survived+1))
  else
    echo "caught:   $claim"; caught=$((caught+1))
  fi
  mv "$file.orig" "$file"
done
echo "mutants caught: $caught/$((caught+survived))"
[ "$survived" -eq 0 ]
