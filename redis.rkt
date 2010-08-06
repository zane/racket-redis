;;; redis.rkt
;;;
;;; Implements an interface to the redis persistent key-value
;;; database. Communicates with the database through its TCP
;;; interface.
#lang racket

(require racket/date)

(define-struct connection (in out cust))

(define (connect)
  (let ([cust (make-custodian)])
    (parameterize ([current-custodian cust])
      (let-values ([(in out) (tcp-connect "localhost" 6379)])
        (make-connection in out cust)))))

(define current-connection (make-parameter (connect)))

(define (disconnect!)
  (define conn (current-connection))
  (with-handlers ([exn:fail:network? void])
    (close-output-port (connection-out conn)))
  (with-handlers ([exn:fail:network? void])
    (close-input-port (connection-in conn))))

(define send-command
  (lambda commands
    (define out (connection-out (current-connection)))
    (fprintf out "*~a\r\n" (length commands))
    (for-each (lambda (command)
                (fprintf out "$~a\r\n~a\r\n"
                         (bytes-length
                          (string->bytes/utf-8
                           command))
                         command))
              commands)
    (flush-output out)))

(define-struct exn:redis (message))

(define (read-reply)
  (define in (connection-in (current-connection)))
  (match (read-bytes 1 in)
    [#"-" (read-line in 'return-linefeed)]
    [#"+" (read-line in 'return-linefeed)]
    [#"$" (read-bulk-reply in)]
    [#"*" (read-multi-bulk-reply in)]
    [#":" (string->number (read-line in 'return-linefeed))]
    [_ (raise (make-exn:redis (format "invalid control character: ~a"
                                      (read-byte in))))]))

(define (read-bulk-reply in)
  (flush-output)
  (let ([length (string->number (read-line in 'return-linefeed))])
    (begin0 (read-bytes length in)
      (read-line in 'return-linefeed))))

(define (read-multi-bulk-reply in)
  (let ([length (string->number (read-line in 'return-linefeed))])
    (flush-output)
    (build-list length
                (lambda (_) (read-reply)))))
    

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; COMMANDS

(define (ping)
  (send-command "PING")
  (read-reply))

;; Connection handling

(define (quit)
  (send-command "QUIT")
  (disconnect!))

(define (auth)
  (send-command "AUTH")
  (read-reply))

;; Commands operating on all value types

(define (exists? key)
  (send-command "EXISTS" key) 
  (match (read-reply)
         [1 #t]
         [0 #f]))

(define del!
  (lambda keys
    (apply send-command `("DEL" ,@keys))
    (read-reply)))

(define (type key)
  (send-command "TYPE" key)
  (string->symbol (read-reply)))

(define (keys pattern)
  (send-command "KEYS" pattern)
  (read-reply))

(define (randomkey)
  (send-command "RANDOMKEY")
  (read-reply))


(define (rename! oldkey newkey)
  (send-command "RENAME" oldkey newkey)
  (read-reply))

(define (renamenx! oldkey newkey)
  (send-command "RENAME" oldkey newkey)
  (match (read-reply)
         [1 #t]
         [0 #f]))

(define (dbsize)
  (send-command "DBSIZE")
  (read-reply))

(define (expire! key seconds)
  (send-command "EXPIRE" key seconds)
  (read-reply))

(define (expireat! key date)
  (send-command "EXPIREAT" key (date->seconds date))
  (read-reply))

(define (ttl key)
  (send-command "TTL" key)
  (read-reply))

(define (select key)
  (send-command "SELECT" key)
  (read-reply))

(define (move key dbindex)
  (send-command "MOVE" key dbindex)
  (read-reply))

(define (flushdb)
  (send-command "FLUSHDB")
  (read-reply))

(define (flushall)
  (send-command "FLUSHALL")
  (read-reply))

(define (set key value)
  (send-command "SET" key value)
  (read-reply))

(define (get key)
  (send-command "GET" key)
  (read-reply))

(define (getset key value)
  (send-command "GETSET" key value)
  (read-reply))

(define mget
  (lambda keys
    (apply send-command `("MGET" ,@keys)
    (read-reply))))
