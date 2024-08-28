// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
)

var (
	invalidCharRegex      = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	invalidFirstCharRegex = regexp.MustCompile(`^[^a-zA-Z_]`)
)

func LabelMapToString(labels map[string]string) string {
	var builder strings.Builder
	keys := maps.Keys(labels)
	sort.Strings(keys)
	for _, key := range keys {
		value := labels[key]
		if len(builder.String()) > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(Sanitize(key))
		builder.WriteString("=\"")
		builder.WriteString(Sanitize(value))
		builder.WriteString("\"")
	}
	return builder.String()
}

// Sanitize replaces all invalid characters for metric labels with an
// underscore. The first character must be a letter or underscore or else it
// will also be replaced with an underscore.
func Sanitize(input string) string {
	sanitized := invalidCharRegex.ReplaceAllString(input, "_")
	sanitized = invalidFirstCharRegex.ReplaceAllString(sanitized, "_")
	return sanitized
}

const TimeFormat = "2006-01-02T15_04_05"

func SplitArgsAtDash(cmd *cobra.Command, args []string) (before, after []string) {
	argsLenAtDash := cmd.ArgsLenAtDash()
	if argsLenAtDash < 0 {
		// If there's no dash, the value of this is -1.
		before = args[:len(args):len(args)]
	} else {
		// NB: Have to do this verbose slicing to force Go to copy the
		// memory. Otherwise, later `append`s will break stuff.
		before = args[0:argsLenAtDash:argsLenAtDash]
		after = args[argsLenAtDash:len(args):len(args)]
	}
	return
}

// VerifyPathFlag verifies that the given path flag points to a file or
// directory, depending on the expectDir flag.
func VerifyPathFlag(flagName, path string, expectDir bool) error {
	if fi, err := os.Stat(path); err != nil {
		return fmt.Errorf("the %s flag points to a path %s that does not exist", flagName, path)
	} else {
		switch isDir := fi.Mode().IsDir(); {
		case expectDir && !isDir:
			return fmt.Errorf("the %s flag must point to a directory not a file", flagName)
		case !expectDir && isDir:
			return fmt.Errorf("the %s flag must point to a file not a directory", flagName)
		}
	}
	return nil
}

// GetRegexExclusionPairs returns a list of regex exclusion pairs, separated by
// comma, derived from the command flags. The first element of the pair is the
// package regex and the second is the microbenchmark regex.
func GetRegexExclusionPairs(excludeList []string) [][]*regexp.Regexp {
	excludeRegexes := make([][]*regexp.Regexp, 0)
	for _, pair := range excludeList {
		pairSplit := strings.Split(pair, ":")
		var pkgRegex, benchRegex *regexp.Regexp
		if len(pairSplit) != 2 {
			pkgRegex = regexp.MustCompile(".*")
			benchRegex = regexp.MustCompile(pairSplit[0])
		} else {
			pkgRegex = regexp.MustCompile(pairSplit[0])
			benchRegex = regexp.MustCompile(pairSplit[1])
		}
		excludeRegexes = append(excludeRegexes, []*regexp.Regexp{pkgRegex, benchRegex})
	}
	return excludeRegexes
}

func InitRoachprod() {
	_ = roachprod.InitProviders()
}

func RoachprodRun(clusterName string, l *logger.Logger, cmdArray []string) error {
	return roachprod.Run(
		context.Background(), l, clusterName, "", "", false,
		os.Stdout, os.Stderr, cmdArray, install.DefaultRunOptions(),
	)
}

func InitLogger(path string) *logger.Logger {
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	var loggerError error
	l, loggerError := loggerCfg.NewLogger(path)
	if loggerError != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unable to configure logger: %s\n", loggerError)
		os.Exit(1)
	}
	return l
}
