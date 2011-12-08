#!/usr/bin/env ruby

require 'net/ssh'

if ARGV.length != 2
  puts "Usage: #{$0} <range> <username>"
  puts "       Kill all the user process"
  puts "Example: #{$0} 131.114.11.85-255 foobar"
  exit
end

range = ARGV[0]
uname = ARGV[1]

`nmap -p 22 --open -oG - #{range} -Pn | grep ssh`.each_line do |line|
  host = line.split(' ')[1]

  begin
    Net::SSH.start(host, uname) do |session|
      puts "Killing everything on #{host}"
      session.exec "kill -9 -1"
    end
  rescue Exception => e
  end
end
