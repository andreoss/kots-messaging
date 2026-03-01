object ScalacOptions {

  def forVersion(version: String): List[String] =
    if (version.startsWith("3")) List("-deprecation")
    else List("-deprecation", "-Ytasty-reader")
}
