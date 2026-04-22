/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.bundle;

/**
 * Metadata for the Lance Spark bundle targeting Spark 3.5 and Scala 2.12.
 *
 * <p>The bundle is a shaded, fat JAR that packages the Lance Spark connector together with all of
 * its transitive dependencies for drop-in use.
 */
public final class BundleInfo {

  // Not instantiable — access members statically.
  private BundleInfo() {}

  /** Returns the Maven artifact name of this bundle. */
  public static String getBundleName() {
    return "lance-spark-bundle-3.5_2.12";
  }

  /**
   * Parses the Spark version (e.g. {@code "3.5"}) out of a bundle artifact name of the form {@code
   * lance-spark-bundle-<spark>_<scala>}.
   *
   * @throws IllegalArgumentException if {@code bundleName} does not match that shape
   */
  public static String parseSparkVersion(String bundleName) {
    int scalaSep = bundleName.lastIndexOf('_');
    int sparkSep = bundleName.lastIndexOf('-');
    if (scalaSep < 0 || sparkSep < 0 || sparkSep >= scalaSep) {
      throw new IllegalArgumentException("Not a Lance Spark bundle artifact: " + bundleName);
    }
    String version = bundleName.substring(sparkSep + 1, scalaSep);
    if (version.isEmpty()) {
      throw new IllegalArgumentException("Missing Spark version in: " + bundleName);
    }
    return version;
  }

  /** Returns the major Spark version (e.g. {@code 3} for {@code "3.5"}) for this bundle. */
  public static int getSparkMajorVersion() {
    String version = parseSparkVersion(getBundleName());
    int dot = version.indexOf('.');
    return Integer.parseInt(dot < 0 ? version : version.substring(0, dot));
  }
}
