<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Sam\Symfony\Bridge\EnqueueMessage;

use Interop\Queue\PsrContext;

/**
 * It is reponsible of managing the queue context. It will ensure the queue is successfully created
 * and is ready to work.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
interface ContextManager
{
    /**
     * Returns the associated `PsrContext` object.
     *
     * @return PsrContext
     */
    public function psrContext(): PsrContext;

    /**
     * Recover from the given exception. This can typically be something like the queue or topic do not exists.
     *
     * Returns `true` if it did manage to recover and `false` if it can't.
     *
     * @param \Exception $exception
     * @param array      $destination
     *
     * @return bool
     */
    public function recoverException(\Exception $exception, array $destination): bool;

    /**
     * Ensure that the given destination exists.
     *
     * In the example of AMQP, it will create the topic, queue & binding.
     *
     * @param array $destination
     *
     * @return bool
     */
    public function ensureExists(array $destination): bool;
}
