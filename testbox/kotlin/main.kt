import java.util.Scanner

fun main(args: Array<String>) {
    val n = readLine() ?: ""
    val str = readLine() ?: ""
    var result = ""
    val chars = str.toCharArray()
    var i = 0
    while (i < chars.size) {
        var sum = 0
        var zero = ""
        // count consecutive '1's
        while (i < chars.size && chars[i] == '1') {
            sum++
            i++
        }
        // count single separator zero (or more zeros representing zeros digits)
        while (i < chars.size && chars[i] == '0') {
            zero += "0"
            i++
        }
        result += sum
        result += zero
    }
    println(result)
}