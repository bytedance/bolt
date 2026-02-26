// Copyright (c) ByteDance Ltd. and/or its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/apache/skywalking-eyes/pkg/config"
	"github.com/apache/skywalking-eyes/pkg/header"
	"github.com/apache/skywalking-eyes/pkg/logger"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func main() {
	files := os.Args[1:]
	if len(files) == 0 {
		fmt.Printf("[INFO] No files to check.\n")
		return
	}
	cwd, err := os.Getwd()

	if err != nil {
		panic(err)
	}

	projectRoot, err := filepath.Abs(filepath.Join(cwd, "../../"))
	if err != nil {
		panic(err)
	}
	configFile := filepath.Join(projectRoot, ".licenserc.yaml")

	fmt.Printf("[INFO] Project Root: %s\n", projectRoot)
	fmt.Printf("[INFO] Config Path:  %s\n", configFile)

	// read and parse config file
	logger.Log.SetLevel(logrus.InfoLevel)
	config, err := config.NewConfigFromFile(configFile)
	if err != nil {
		fmt.Printf("[WARNING] Failed to load config: %v\n", err)
	}
	if len(config.Headers()) > 1 {
		fmt.Printf("[ERROR] Only one header configuration is supported.\n")
		os.Exit(1)
	}
	// move up to project root
	if err := os.Chdir("../../"); err != nil {
		fmt.Printf("[ERROR] Failed to change dir to project root: %v\n", err)
		os.Exit(1)
	}

	licenseHeaderConfig := config.Headers()[0]

	// 3. filter files
	// iterate over passed files and filter out ignored files
	validFiles := []string{}
	for _, file := range files {
		if ignore, err := licenseHeaderConfig.ShouldIgnore(file); err == nil && !ignore {
			validFiles = append(validFiles, file)
		} else if err != nil {
			fmt.Printf("[ERROR] Failed to check file %s: %v\n", file, err)
			os.Exit(1)
		}
	}

	if len(validFiles) == 0 {
		fmt.Printf("[INFO] All files are ignored.\n")
		os.Exit(0)
	}

	g := new(errgroup.Group)
	g.SetLimit(runtime.GOMAXPROCS(0))
	result := header.Result{}
	for _, file := range validFiles {
		f := file
		g.Go(func() error {
			return header.CheckFile(f, licenseHeaderConfig, &result)
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Printf("[ERROR] License check failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("[INFO] License check passed successfully\n")
}
