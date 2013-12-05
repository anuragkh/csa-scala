package csa

object Main {
    def main(args: Array[String]) {
        if (args.length != 2) {
            System.err.println("Usage: CSA <master> <filename>")
            System.exit(1)
        }
        val index = new CSA(args(0), args(1))
    }
}