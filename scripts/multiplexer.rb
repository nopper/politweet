#!/usr/bin/env ruby

require 'net/ssh'

# The range is 

if ARGV.length != 5
  puts "Usage: #{$0} <range> <username> <startport> <cmd> <outfile>"
  puts ""
  puts "       Do a scan over the range checking for SSH"
  puts "       connection. Than connect to it and execute"
  puts "       the socks server"
  puts ""
  puts "Example: #{$0} 131.114.11.85-255 foobar 8000 'screen Documents/3proxy-0.6.1/src/socks -p8080 -l' proxylist"
  exit
end

threads = []
sessions = []

range = ARGV[0]
uname = ARGV[1]
cport = ARGV[2].to_i
cmd   = ARGV[3]
file  = ARGV[4]

File.open(file, 'w+') do |f|
  `nmap -p 22 --open -oG - #{range} -Pn | grep ssh`.each_line do |line|
    host = line.split(' ')[1]

    f.write "0:140:3600:socks5:#{cport}:localhost\n"
    cport += 1

    threads << Thread.new(host, cport) do |myhost, myport|
      begin
        sessions << Net::SSH.start(myhost, uname) do |session|
          puts "Spawning listener #{myhost}:#{myport}"
          session.forward.local(myport, 'localhost', 8080)
          session.exec "#{cmd}"
        end
      rescue Exception => e
        puts "Got killed?"
      end
    end
  end
end
threads.each { |thread| thread.join }

