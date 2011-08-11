import os
import sys

def process(text):
    text = "/**\n" + text;
    text = text.replace("///", " *")
    text = text.replace("<summary>", "")
    text = text.replace("</summary>", "")
    text = text.replace("<param name=\"", "@param ")
    text = text.replace("</param>", "")
    text = text.replace("<returns>", "@return ")
    text = text.replace("</returns>", "")
    text = text.replace("<exception cref=\"", "@exception ")
    text = text.replace("<remarks>", "<p>")
    text = text.replace("</remarks>", "")
    text = text.replace("<para>", "<p>")
    text = text.replace("</para>", "<p>")
    text = text.replace("<example>", "<p>\n * Example:\n * ")
    text = text.replace("</example>", "\n * ")
    text = text.replace("<c>", "<code>")
    text = text.replace("</c>", "</code>")
    text = text.replace("<code>", "<pre>")
    text = text.replace("</code>", "</pre>")
    text = text.replace("<list type=\"bullet\">", "<ul>")
    text = text.replace("</list>", "</ul>")
    text = text.replace("<item>", "<li>")
    text = text.replace("</item>", "</li>")
    text = text.replace("<seealso cref=\"", "@see ")
    text = text.replace("<see cref=\"", "<a href=\"")
    text = text.replace("</see>", "</a>")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("<p>\n * <p>", "<p>")
    text = text.replace("<p>\n * <p>", "<p>")
    text = text.replace("\">", " ")
    text = text.replace("\"/>", " ")
    text += "\n */"
    return text
    
file = open(sys.argv[1])
text = ""
for line in file:
    line = line.lstrip()
    text += line
text = process(text)
print(text)


